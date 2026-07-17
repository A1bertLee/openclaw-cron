import type { AgentEventPayload } from "../infra/agent-events.js";

export type CronLiveRunState = "running";

export type CronLiveRunEvent = {
  /** Monotonic server-owned replay cursor; never the agent transport seq. */
  seq: number;
  stream: AgentEventPayload["stream"];
  ts: number;
  data: Record<string, unknown>;
};

type CronLiveRun = {
  taskRunId: string;
  jobId: string;
  startedAtMs: number;
  state: CronLiveRunState;
  agentId?: string;
  sessionId?: string;
  sessionKey?: string;
  agentRunId?: string;
  events: CronLiveRunEvent[];
  lastSeq: number;
};

export type CronLiveRunSnapshot = Omit<CronLiveRun, "events"> & {
  events: CronLiveRunEvent[];
  oldestSeq: number;
  hasMoreBefore: boolean;
};

export type CronLiveRunEventStore = ReturnType<typeof createCronLiveRunEventStore>;

const DEFAULT_MAX_EVENTS_PER_RUN = 200;
const DEFAULT_MAX_TEXT_CHARS = 4_000;
const SENSITIVE_KEY = /(?:token|secret|password|authorization|cookie|api[_-]?key|access[_-]?key)/i;
const BLOCKED_DATA_KEYS = new Set([
  "args",
  "input",
  "result",
  "partialResult",
  "command",
  "cwd",
  "env",
]);
const DATA_KEYS_BY_STREAM: Partial<Record<AgentEventPayload["stream"], readonly string[]>> = {
  assistant: ["text", "delta", "replace", "phase"],
  thinking: ["text", "delta", "replace", "phase"],
  tool: ["toolCallId", "name", "phase", "title", "status", "summary", "error"],
  item: [
    "itemId",
    "phase",
    "kind",
    "title",
    "status",
    "name",
    "meta",
    "toolCallId",
    "error",
    "summary",
    "progressText",
  ],
  lifecycle: ["phase", "status", "error", "stopReason"],
  error: ["message", "error", "code", "kind"],
  command_output: [
    "itemId",
    "phase",
    "title",
    "toolCallId",
    "name",
    "output",
    "status",
    "exitCode",
    "durationMs",
  ],
  patch: [
    "itemId",
    "phase",
    "title",
    "toolCallId",
    "name",
    "added",
    "modified",
    "deleted",
    "summary",
  ],
};

function sanitizeText(value: string, maxTextChars: number): string {
  const redacted = value
    .replace(/\b(Bearer\s+)[^\s]+/gi, "$1[REDACTED]")
    .replace(/\b(token|secret|password|api[_-]?key)\s*[:=]\s*[^\s,;]+/gi, "$1=[REDACTED]");
  return redacted.length > maxTextChars ? `${redacted.slice(0, maxTextChars)}…` : redacted;
}

function sanitizeValue(value: unknown, maxTextChars: number, truncateText: boolean): unknown {
  if (typeof value === "string") {
    return sanitizeText(value, truncateText ? maxTextChars : Math.max(maxTextChars, 256));
  }
  if (typeof value === "number" || typeof value === "boolean" || value === null) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.slice(0, 50).map((entry) => sanitizeValue(entry, maxTextChars, truncateText));
  }
  return undefined;
}

function sanitizeData(
  data: Record<string, unknown>,
  stream: AgentEventPayload["stream"],
  maxTextChars: number,
) {
  const allowed = DATA_KEYS_BY_STREAM[stream] ?? [];
  const sanitized: Record<string, unknown> = {};
  for (const key of allowed) {
    if (SENSITIVE_KEY.test(key) || BLOCKED_DATA_KEYS.has(key)) {
      continue;
    }
    const value = sanitizeValue(
      data[key],
      maxTextChars,
      ["text", "delta", "output", "summary", "error", "meta", "progressText", "message"].includes(
        key,
      ),
    );
    if (value !== undefined) {
      sanitized[key] = value;
    }
  }
  return sanitized;
}

/**
 * Process-local live-run journal. The Gateway constructs and injects one store
 * per runtime; this module deliberately owns no singleton state.
 */
export function createCronLiveRunEventStore(options?: {
  maxEventsPerRun?: number;
  maxTextChars?: number;
}) {
  const maxEventsPerRun = Math.max(1, options?.maxEventsPerRun ?? DEFAULT_MAX_EVENTS_PER_RUN);
  const maxTextChars = Math.max(1, options?.maxTextChars ?? DEFAULT_MAX_TEXT_CHARS);
  const runsByTaskRunId = new Map<string, CronLiveRun>();
  const taskRunIdBySessionKey = new Map<string, string>();
  const taskRunIdByAgentRunId = new Map<string, string>();
  let nextSeq = 0;

  const resolveRun = (event: AgentEventPayload): CronLiveRun | undefined => {
    const taskRunId =
      (event.sessionKey ? taskRunIdBySessionKey.get(event.sessionKey) : undefined) ??
      taskRunIdByAgentRunId.get(event.runId);
    return taskRunId ? runsByTaskRunId.get(taskRunId) : undefined;
  };

  const removeBindings = (run: CronLiveRun) => {
    if (run.sessionKey) taskRunIdBySessionKey.delete(run.sessionKey);
    if (run.agentRunId) taskRunIdByAgentRunId.delete(run.agentRunId);
  };

  return {
    register(params: {
      taskRunId: string;
      jobId: string;
      startedAtMs: number;
      agentId?: string;
      sessionId?: string;
      sessionKey?: string;
      agentRunId?: string;
    }) {
      const run: CronLiveRun = { ...params, state: "running", events: [], lastSeq: nextSeq };
      runsByTaskRunId.set(params.taskRunId, run);
      if (params.sessionKey) taskRunIdBySessionKey.set(params.sessionKey, params.taskRunId);
      if (params.agentRunId) taskRunIdByAgentRunId.set(params.agentRunId, params.taskRunId);
    },

    bindExecution(params: {
      taskRunId: string;
      agentId?: string;
      sessionId?: string;
      sessionKey?: string;
      agentRunId?: string;
    }) {
      const run = runsByTaskRunId.get(params.taskRunId);
      if (!run) return false;
      Object.assign(run, params);
      if (params.sessionKey) taskRunIdBySessionKey.set(params.sessionKey, params.taskRunId);
      if (params.agentRunId) taskRunIdByAgentRunId.set(params.agentRunId, params.taskRunId);
      return true;
    },

    appendAgentEvent(event: AgentEventPayload) {
      const run = resolveRun(event);
      if (!run) return false;
      run.agentRunId ??= event.runId;
      taskRunIdByAgentRunId.set(event.runId, run.taskRunId);
      const seq = ++nextSeq;
      run.lastSeq = seq;
      run.events.push({
        seq,
        stream: event.stream,
        ts: event.ts,
        data: sanitizeData(event.data, event.stream, maxTextChars),
      });
      if (run.events.length > maxEventsPerRun)
        run.events.splice(0, run.events.length - maxEventsPerRun);
      return true;
    },

    finish(taskRunId: string) {
      const run = runsByTaskRunId.get(taskRunId);
      if (!run) return false;
      removeBindings(run);
      runsByTaskRunId.delete(taskRunId);
      return true;
    },

    list() {
      return [...runsByTaskRunId.values()]
        .map((run) => ({
          ...run,
          events: [...run.events],
          oldestSeq: run.events[0]?.seq ?? run.lastSeq + 1,
          hasMoreBefore: false,
        }))
        .sort((a, b) => b.startedAtMs - a.startedAtMs || a.taskRunId.localeCompare(b.taskRunId));
    },

    read(params: { taskRunId: string; afterSeq?: number }): CronLiveRunSnapshot | undefined {
      const run = runsByTaskRunId.get(params.taskRunId);
      if (!run) return undefined;
      const afterSeq = Math.max(0, params.afterSeq ?? 0);
      const oldestSeq = run.events[0]?.seq ?? run.lastSeq + 1;
      return {
        ...run,
        events: run.events.filter((event) => event.seq > afterSeq),
        oldestSeq,
        hasMoreBefore: afterSeq < oldestSeq - 1,
      };
    },
  };
}
