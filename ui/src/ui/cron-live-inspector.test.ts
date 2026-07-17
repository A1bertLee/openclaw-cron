// @vitest-environment node
import { describe, expect, it } from "vitest";
import {
  appendCronLiveAgentEvent,
  createCronLiveInspectorState,
  getCronLiveReplayEvents,
} from "./cron-live-inspector.ts";

describe("cron live inspector", () => {
  it("captures live agent events from an isolated cron run without accepting ordinary chat events", () => {
    const state = createCronLiveInspectorState();

    const ignored = appendCronLiveAgentEvent(state, {
      runId: "chat-run",
      seq: 1,
      stream: "tool",
      ts: 1,
      sessionKey: "agent:main:main",
      data: { name: "web_search", phase: "start" },
    });
    const accepted = appendCronLiveAgentEvent(state, {
      runId: "cron-run",
      seq: 1,
      stream: "tool",
      ts: 2,
      sessionKey: "agent:main:cron:job-1:run:session-1",
      data: { name: "web_search", phase: "start" },
    });

    expect(ignored).toBe(false);
    expect(accepted).toBe(true);
    expect(state.runs).toEqual([
      {
        runId: "cron-run",
        jobId: "job-1",
        sessionKey: "agent:main:cron:job-1:run:session-1",
        startedAt: 2,
        lastEventAt: 2,
        events: [
          {
            seq: 1,
            stream: "tool",
            ts: 2,
            summary: "web_search started",
            status: "running",
          },
        ],
        replayEvents: [
          {
            runId: "cron-run",
            seq: 1,
            stream: "tool",
            ts: 2,
            sessionKey: "agent:main:cron:job-1:run:session-1",
            data: { name: "web_search", phase: "start" },
          },
        ],
      },
    ]);
    expect(getCronLiveReplayEvents(state, "agent:main:cron:job-1")).toEqual([
      {
        runId: "cron-run",
        seq: 1,
        stream: "tool",
        ts: 2,
        sessionKey: "agent:main:cron:job-1:run:session-1",
        data: { name: "web_search", phase: "start" },
      },
    ]);
  });

  it("coalesces tool lifecycle snapshots into one display entry without dropping replay events", () => {
    const state = createCronLiveInspectorState();
    const base = {
      runId: "cron-run",
      stream: "tool",
      sessionKey: "agent:main:cron:job-1:run:session-1",
      data: { toolCallId: "call-1", name: "exec", phase: "start" },
    };

    appendCronLiveAgentEvent(state, { ...base, seq: 1, ts: 1 });
    appendCronLiveAgentEvent(state, {
      ...base,
      seq: 2,
      ts: 2,
      data: { toolCallId: "call-1", name: "exec", phase: "result" },
    });

    expect(state.runs[0]?.events).toEqual([
      {
        seq: 2,
        stream: "tool",
        ts: 2,
        summary: "exec completed",
        status: "completed",
        toolCallId: "call-1",
      },
    ]);
    expect(state.runs[0]?.replayEvents.map((event) => event.seq)).toEqual([1, 2]);
  });

  it("removes a completed run and its replay buffer before the next run of the same job", () => {
    const state = createCronLiveInspectorState();
    const sessionKey = "agent:main:cron:job-1:run:session-1";

    appendCronLiveAgentEvent(state, {
      runId: "cron-run-1",
      seq: 1,
      stream: "assistant",
      ts: 1,
      sessionKey,
      data: { text: "first execution" },
    });
    appendCronLiveAgentEvent(state, {
      runId: "cron-run-1",
      seq: 2,
      stream: "lifecycle",
      ts: 2,
      sessionKey,
      data: { phase: "end" },
    });
    appendCronLiveAgentEvent(state, {
      runId: "cron-run-2",
      seq: 1,
      stream: "assistant",
      ts: 3,
      sessionKey: "agent:main:cron:job-1:run:session-2",
      data: { text: "second execution" },
    });

    expect(state.runs.map((run) => run.runId)).toEqual(["cron-run-2"]);
    expect(
      getCronLiveReplayEvents(state, "agent:main:cron:job-1").map((event) => event.data),
    ).toEqual([{ text: "second execution" }]);
  });

  it("deduplicates replayed events and bounds the event buffer per run", () => {
    const state = createCronLiveInspectorState({ maxEventsPerRun: 2 });
    const base = {
      runId: "cron-run",
      stream: "item",
      sessionKey: "agent:main:cron:job-1:run:session-1",
    };

    appendCronLiveAgentEvent(state, {
      ...base,
      seq: 1,
      ts: 1,
      data: { title: "Preparing", status: "running" },
    });
    appendCronLiveAgentEvent(state, {
      ...base,
      seq: 1,
      ts: 1,
      data: { title: "Preparing", status: "running" },
    });
    appendCronLiveAgentEvent(state, {
      ...base,
      seq: 2,
      ts: 2,
      data: { title: "Searching", status: "running" },
    });
    appendCronLiveAgentEvent(state, {
      ...base,
      seq: 3,
      ts: 3,
      data: { title: "Finished", status: "completed" },
    });

    expect(state.runs[0]?.events.map((event) => event.seq)).toEqual([2, 3]);
    expect(state.runs[0]?.lastEventAt).toBe(3);
  });
});
