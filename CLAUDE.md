# wallet-service-claude

gRPC + Kafka + OpenTelemetry demo wallet service. Fully async: `wallet-service`
only validates and publishes commands, `wallet-worker` is the sole owner of
state, and clients consume `wallet.results` directly. See `README.md` for the
full architecture writeup.

## Keep the architecture diagram in sync

Whenever a change is architecturally significant — a service is added,
removed, or renamed; a Kafka topic or its schema (`proto/messages.proto`)
changes; the request/response flow between `wallet-client` /
`wallet-service` / `wallet-worker` changes; or `docker-compose.yml` gains or
loses a service — update **both**:

1. The mermaid diagram in the "Архитектура" section of `README.md`.
2. `docs/architecture.html` — the standalone HTML diagram (open directly in a
   browser, no server needed). Keep its SVG boxes/arrows and the "Compose
   reference" table consistent with the new topology.

If Artifact publishing is available in the session, also try to redeploy
`docs/architecture.html` as an Artifact (same file, same URL if one was
already published this conversation) so there's a shareable link, not just
the local file. If publishing 403s or otherwise fails, don't block on it —
the committed `docs/architecture.html` file is the fallback source of truth.

Small, non-architectural changes (bug fixes, test additions, refactors that
don't move a box or an arrow) don't need a diagram update.

## Scratch files

Put temporary/working files in a local `tmp/` directory (gitignored), not the
system-wide scratchpad path.
