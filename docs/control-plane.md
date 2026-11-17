# Stickman Video Director — local control plane foundation

This additive module introduces a dependency-free HTTP control plane and a filesystem-persisted job ledger beside the existing Director and Autopilot workflows. It does not replace the current renderer or style-specific production contracts.

## Included in this increment

- Atomic, durable JSON job records stored under the ignored autopilot-work directory.
- Idempotent job creation when a caller supplies an idempotency key.
- A single-process worker claim endpoint, operator pause/resume controls and validated lifecycle transitions.
- Bearer-token protection for operator and worker routes. The public health endpoint reveals only liveness.
- Credential-like payload keys are rejected; API keys, refresh tokens and passwords must live in a secret manager.
- No new npm dependency and no modification to the existing renderer entrypoint.

## Start locally

Use Node.js 20 or newer from skills/directing-stickman-videos:

    export STICKMAN_CONTROL_TOKEN="$(node -e "console.log(require('node:crypto').randomBytes(32).toString('base64url'))")"
    npm run control-plane

Defaults: host 127.0.0.1, port 4317, state directory autopilot-work/control-plane under the current working directory. Optional settings are STICKMAN_CONTROL_HOST, STICKMAN_CONTROL_PORT and STICKMAN_CONTROL_DIR.

Keep the service on loopback unless it is behind a TLS-terminating reverse proxy and protected private network. Never put the bearer token in source control, project JSON, job payloads or logs.

## API

All /api routes require Authorization: Bearer $STICKMAN_CONTROL_TOKEN.

- GET /health — minimal liveness response.
- GET /api/queue — pause state and job counts.
- GET /api/jobs?status=queued&limit=50 — recent jobs.
- POST /api/jobs — enqueue a job with type, optional projectId, payload and optional idempotencyKey.
- POST /api/jobs/claim — claim the oldest queued job with a workerId; returns job:null while paused or empty.
- POST /api/jobs/{id}/transition — apply a legal lifecycle transition.
- POST /api/control/pause and POST /api/control/resume — stop or resume new claims.

Supported types: ingest, render, export and publish. Lifecycle: queued → running → qa → awaiting_approval → uploading → scheduled or published, with failure/cancellation paths and an explicit retry from failed to queued.

## Limits and preservation guarantees

This is a single-process local control-plane foundation, not yet a distributed queue, scheduler, renderer worker or always-on service. Production still needs multi-process-safe leases, a worker adapter that invokes the existing renderer and FFprobe/QA, a real scheduler, remote storage/backup, monitoring/alerts and deployment hardening. This module does not implement YouTube OAuth or upload.

Existing Director styles and Autopilot renderers remain untouched. OpenMontage remains a preservation requirement and must stay an independent editing/rendering path. Connect it through an explicit adapter only after its source, launch command and input/output contract are available; do not remove or silently replace it with this control plane.

## Tests

Run:

    node --test tests/control-plane.test.mjs

The tests cover persistence, idempotency, pause/claim, lifecycle validation, credential rejection and API authentication. They do not certify a full video render, remote deployment, OAuth or unattended publishing.
