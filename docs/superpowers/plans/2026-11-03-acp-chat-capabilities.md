# ACP chat capabilities

1. Extend the ACP client to negotiate and publish session capabilities, configuration, usage, structured tool content and session identity. Route configuration and legacy mode changes to the agent; restore a supported session on transport reconnect.
2. Carry the latest session metadata through each engine's bounded event transport and board projection, and validate configuration and attachment input at the API boundary.
3. Add capability-aware chat controls for configuration (including permission modes), context usage, attachments and richer tool output. Keep unsupported choices hidden and preserve each session's state separately.
4. Exercise protocol messages with a deterministic ACP subprocess, test backend projections and input validation, and test the real browser UI and all affected automated suites.
