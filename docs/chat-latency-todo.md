# Chat Latency TODO

Goal: make ordinary chat feel immediate without doing a large routing/orchestration refactor in one pass.

## Guardrails

- Keep ordinary chat fast by avoiding extra model calls unless structured product evidence requires them.
- Do not infer task splitting or routing from hardcoded natural-language phrases.
- Prefer typed state, explicit UI/slash/operator actions, attachment metadata, URL/domain evidence, tool schemas, task-frame state, and model-produced structured plans.
- Ship these as small, separately testable changes. Avoid a single broad streaming/cache/LangGraph rewrite.
- Preserve final user-visible result delivery even when live activity or partial streaming is enabled.

## Follow-Ups

1. Add phase timing for Web chat preflight.
   - Replace the single long "Preparing request" span with typed phases such as checking attachments, checking task state, building context, starting model, running tools, preparing artifacts, and saving conversation.
   - Log phase durations on slow turns so prod incidents can identify the exact slow section.
   - Keep the UI text concise and avoid exposing internal paths, task prose, credentials, connector payloads, or raw tool output.

2. Stream the first model response to Web when the orchestrator supports it.
   - End "Preparing request" as soon as the first answer chunk or first model-start event is available.
   - Keep persistence, memory capture, reflection, and history writes after the user-visible response where safe.
   - Ensure approval, artifact, mini-agent, and delegated-task paths still show terminal success/failure messages.

3. Make prompt assembly prompt-cache-friendly.
   - Keep stable system/tool instructions in a deterministic prefix.
   - Put dynamic pieces, such as user turn, recent history, runtime facts, and artifact context, after the stable prefix.
   - Avoid unnecessary churn in ordering, timestamps, rendered settings text, and tool descriptions.

4. Cache rendered context sections in-process. **Done for Web stable context prefix.**
   - Candidate sections: system capability snapshot, runtime config prompt, workspace connections prompt, enabled skill-pack prompt, installed dependency context, and web research guidance.
   - Invalidate on settings changes, connection changes, skill-pack install/update/remove, tool-registry changes, or runtime config saves.
   - Keep memory/history/task-frame context uncached or short-lived because it changes per turn.
   - Current implementation caches only the stable Web prompt prefix and keeps user history, memory, task frames, recent tool context, attachments, and current user text uncached.
   - Runtime DB mtime is intentionally excluded from the cache key because ordinary conversation writes update that file every turn.

5. Use LangGraph for typed preflight state where it reduces repeated work.
   - Model Web chat preflight as typed nodes for structured action, attachments, task-frame state, context sections, tool-scope evidence, and final orchestrator input.
   - Use checkpoints/node caching for deterministic stable nodes only.
   - Do not use LangGraph as another prose classifier. Branch on typed state and verified runtime facts.

6. Add regression coverage for latency behavior.
   - Prove casual chat with no structured evidence skips extra classifiers and remains eligible for the interactive fast profile.
   - Prove structured cases still work: attachments, URLs, open task frames, explicit config actions, artifact follow-ups, and approval/task continuation.
   - Add a Web boundary test that verifies phase activity arrives before the final response for a slow model path.

## References

- OpenAI latency optimization: fewer requests, faster models where appropriate, streaming, and prompt-cache-friendly stable prefixes.
- OpenAI prompt caching: static prompt content should appear early and remain stable.
- LangGraph persistence/node caching: useful for typed deterministic workflow state, not free-form text routing.
