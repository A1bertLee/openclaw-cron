// Control UI helpers for collecting live agent events from isolated Cron run sessions.

export type CronLiveAgentEvent = {
  runId?: unknown;
  seq?: unknown;
  stream?: unknown;
  ts?: unknown;
  sessionKey?: unknown;
  agentId?: unknown;
  data?: unknown;
};

export type CronLiveReplayEvent = {
  runId: string;
  seq: number;
  stream: string;
  ts: number;
  sessionKey: string;
  agentId?: string;
  data?: unknown;
};

export type CronLiveEvent = {
  seq: number;
  stream: string;
  ts: number;
  summary: string;
  status: "running" | "completed" | "failed";
  /** Identifies a tool lifecycle for display-only coalescing. */
  toolCallId?: string;
};

export type CronLiveRun = {
  runId: string;
  jobId: string;
  sessionKey: string;
  startedAt: number;
  lastEventAt: number;
  events: CronLiveEvent[];
  replayEvents: CronLiveReplayEvent[];
};

export type CronLiveInspectorState = {
  maxEventsPerRun: number;
  runs: CronLiveRun[];
};

const DEFAULT_MAX_EVENTS_PER_RUN = 200;
const MAX_TRACKED_RUNS = 20;

export function createCronLiveInspectorState(params?: {
  maxEventsPerRun?: number;
}): CronLiveInspectorState {
  return {
    maxEventsPerRun: Math.max(1, Math.floor(params?.maxEventsPerRun ?? DEFAULT_MAX_EVENTS_PER_RUN)),
    runs: [],
  };
}

function parseCronJobId(sessionKey: string): string | undefined {
  const match = /:cron:([^:]+):run:[^:]+$/u.exec(sessionKey);
  return match?.[1];
}

/** Resolves an isolated Cron run key to the stable task session key used by Chat. */
export function cronParentSessionKeyFromRun(runSessionKey: string): string | undefined {
  const marker = ":run:";
  const index = runSessionKey.lastIndexOf(marker);
  if (index < 1 || !parseCronJobId(runSessionKey)) {
    return undefined;
  }
  return runSessionKey.slice(0, index);
}

/** Returns true when `runSessionKey` is an isolated Cron child of the selected task session. */
export function isCronRunSessionForParent(
  parentSessionKey: string,
  runSessionKey: string,
): boolean {
  return cronParentSessionKeyFromRun(runSessionKey) === parentSessionKey.trim();
}

function asPositiveInteger(value: unknown): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : undefined;
}

function asTimestamp(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}

function toolCallIdFromData(value: unknown): string | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  const toolCallId = (value as Record<string, unknown>).toolCallId;
  return typeof toolCallId === "string" && toolCallId.trim() ? toolCallId.trim() : undefined;
}

function summarizeEvent(
  stream: string,
  data: unknown,
): {
  summary: string;
  status: CronLiveEvent["status"];
} {
  const record = data && typeof data === "object" && !Array.isArray(data) ? data : {};
  const value = record as Record<string, unknown>;
  const title = typeof value.title === "string" ? value.title : undefined;
  const name = typeof value.name === "string" ? value.name : undefined;
  const phase = typeof value.phase === "string" ? value.phase : undefined;
  const error = typeof value.error === "string" ? value.error : undefined;
  const status =
    value.status === "failed" || stream === "error"
      ? "failed"
      : value.status === "completed" || phase === "end" || phase === "result"
        ? "completed"
        : "running";
  const subject = title ?? name ?? stream;
  if (error) {
    return { summary: `${subject}: ${error}`, status: "failed" };
  }
  if (phase === "start") {
    return { summary: `${subject} started`, status };
  }
  if (phase === "end" || phase === "result") {
    return { summary: `${subject} completed`, status };
  }
  const summary = typeof value.summary === "string" ? value.summary : undefined;
  return { summary: summary ? `${subject}: ${summary}` : subject, status };
}

/** Adds one isolated-Cron agent event to the bounded per-run client-side live buffer. */
export function appendCronLiveAgentEvent(
  state: CronLiveInspectorState,
  rawEvent: unknown,
): boolean {
  const event =
    rawEvent && typeof rawEvent === "object" && !Array.isArray(rawEvent)
      ? (rawEvent as CronLiveAgentEvent)
      : null;
  if (!event) {
    return false;
  }
  const runId = typeof event.runId === "string" ? event.runId.trim() : "";
  const sessionKey = typeof event.sessionKey === "string" ? event.sessionKey.trim() : "";
  const jobId = parseCronJobId(sessionKey);
  const seq = asPositiveInteger(event.seq);
  const ts = asTimestamp(event.ts);
  const stream = typeof event.stream === "string" ? event.stream.trim() : "";
  const agentId = typeof event.agentId === "string" ? event.agentId.trim() : "";
  if (!runId || !jobId || !seq || ts === undefined || !stream) {
    return false;
  }

  let run = state.runs.find((entry) => entry.runId === runId);
  if (!run) {
    run = {
      runId,
      jobId,
      sessionKey,
      startedAt: ts,
      lastEventAt: ts,
      events: [],
      replayEvents: [],
    };
    state.runs.unshift(run);
    state.runs.splice(MAX_TRACKED_RUNS);
  }
  if (run.replayEvents.some((entry) => entry.seq === seq)) {
    return false;
  }

  const details = summarizeEvent(stream, event.data);
  const toolCallId = toolCallIdFromData(event.data);
  const displayEvent = { seq, stream, ts, ...details, ...(toolCallId ? { toolCallId } : {}) };
  const previousToolEvent = toolCallId
    ? run.events.find((entry) => entry.toolCallId === toolCallId)
    : undefined;
  if (previousToolEvent) {
    Object.assign(previousToolEvent, displayEvent);
  } else {
    run.events.push(displayEvent);
  }
  run.replayEvents.push({
    runId,
    seq,
    stream,
    ts,
    sessionKey,
    ...(agentId ? { agentId } : {}),
    ...(event.data === undefined ? {} : { data: event.data }),
  });
  run.events.sort((a, b) => a.seq - b.seq);
  run.replayEvents.sort((a, b) => a.seq - b.seq);
  if (run.events.length > state.maxEventsPerRun) {
    const overflow = run.events.length - state.maxEventsPerRun;
    run.events.splice(0, overflow);
    run.replayEvents.splice(0, overflow);
  }
  run.lastEventAt = ts;
  if (stream === "lifecycle" && details.status !== "running") {
    state.runs.splice(state.runs.indexOf(run), 1);
  }
  return true;
}

/** Returns the buffered isolated-run events that belong to a parent Cron session. */
export function getCronLiveReplayEvents(
  state: CronLiveInspectorState | undefined,
  parentSessionKey: string,
): CronLiveReplayEvent[] {
  if (!state || !parentSessionKey.trim()) {
    return [];
  }
  return state.runs
    .filter((run) => cronParentSessionKeyFromRun(run.sessionKey) === parentSessionKey.trim())
    .flatMap((run) => run.replayEvents)
    .toSorted((a, b) => a.ts - b.ts || a.seq - b.seq || a.runId.localeCompare(b.runId));
}
