import { describe, expect, it } from "vitest";
import { createCronLiveRunEventStore } from "./live-run-events.js";

describe("CronLiveRunEventStore", () => {
  it("replays sanitized events after its server-owned cursor for an active task run", () => {
    const store = createCronLiveRunEventStore({ maxEventsPerRun: 2, maxTextChars: 8 });
    store.register({
      taskRunId: "cron:job-1:1",
      jobId: "job-1",
      startedAtMs: 1,
      sessionKey: "agent:main:cron:job-1:run:1",
    });

    store.appendAgentEvent({
      runId: "agent-run-1",
      seq: 99,
      stream: "assistant",
      ts: 10,
      sessionKey: "agent:main:cron:job-1:run:1",
      data: { text: "123456789", token: "do-not-leak" },
    });
    store.appendAgentEvent({
      runId: "agent-run-1",
      seq: 100,
      stream: "tool",
      ts: 11,
      sessionKey: "agent:main:cron:job-1:run:1",
      data: { toolCallId: "call-1", name: "read_file", phase: "start", args: { token: "no" } },
    });

    expect(store.read({ taskRunId: "cron:job-1:1", afterSeq: 1 })).toEqual({
      taskRunId: "cron:job-1:1",
      jobId: "job-1",
      startedAtMs: 1,
      state: "running",
      sessionKey: "agent:main:cron:job-1:run:1",
      agentRunId: "agent-run-1",
      oldestSeq: 1,
      lastSeq: 2,
      hasMoreBefore: false,
      events: [
        {
          seq: 2,
          stream: "tool",
          ts: 11,
          data: { toolCallId: "call-1", name: "read_file", phase: "start" },
        },
      ],
    });
  });

  it("evicts completed runs and reports a retention gap", () => {
    const store = createCronLiveRunEventStore({ maxEventsPerRun: 2 });
    store.register({ taskRunId: "cron:job-1:1", jobId: "job-1", startedAtMs: 1, sessionKey: "s" });
    for (let i = 1; i <= 3; i += 1) {
      store.appendAgentEvent({
        runId: "r",
        seq: i,
        stream: "assistant",
        ts: i,
        sessionKey: "s",
        data: { text: String(i) },
      });
    }
    expect(store.read({ taskRunId: "cron:job-1:1", afterSeq: 0 })?.hasMoreBefore).toBe(true);
    expect(store.finish("cron:job-1:1")).toBe(true);
    expect(store.read({ taskRunId: "cron:job-1:1" })).toBeUndefined();
  });
});
