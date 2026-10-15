# Packaged workflow runner

Build SDK first, then runner. `dist/index.js` exports credential-free
`validateSource(source, { timeoutMs? })`, `sourceHash`, `graphDigest`, and typed IPC
contracts. `dist/cli.js` is the Node entry for validate, graph, and worker modes.

Launch with Node's default JSON IPC protocol, not stdin/argv descriptors:
`fork(cliPath, [], { silent: true, env: { LANG: 'C' } })`. A Go parent can provide a
private socket/pipe as child fd3 with `NODE_CHANNEL_FD=3` and
`NODE_CHANNEL_SERIALIZATION_MODE=json`; messages are one JSON object per newline,
with the same framing used by Node's `child_process.fork`. Send one
`{ mode: 'validate' | 'graph', source }` or `{ mode: 'worker', descriptor }` from
`contracts.ts`. Worker later accepts `{ mode: 'drain' }`; SIGTERM and IPC disconnect
also drain. Default drain10s, configurable1–30000ms; CLI hard stop31s. The Go daemon
must impose independent readiness, memory/output, process-group and kill deadlines.
Discard child stdout/stderr: authored code retains ordinary OS-user capabilities.

Validation runs in a separate child with an environment allowlist (`LANG` only),
5-second default/30-second maximum execution bound and128KiB output bound.
It does not initialize Hatchet. Source is UTF8 SHA256. Graph digest is SHA256 of
compact `{"steps":[{"id":...,"parents":[...]}]}` JSON, sorting IDs with JS string
comparison and parent arrays with JS default sort. Go must implement the same
UTF16 ordering for non-ASCII IDs; do not substitute byte-order sorting.

Worker descriptors carry workspace/tenant/generation, tenant JWT, reachable
`host_port`, explicit TLS/mTLS strategy/server identity and CA/client material
paths, product origin/runner token, and immutable revision source path/hash/graph.
Private credentials/paths belong only on child IPC. JWT subject consistency is
checked locally; engine verifies signature and authorization. Immutable name is
`multica-${workflow_id}-v${version}-${hash.slice(0,12)}`. Logical task name matches
Go `WorkflowActionName`; native Hatchet full action is `${workflowName}:${taskName}`.

Readiness returns normalized graph/digest/actions plus actual engine workflow and
version IDs captured from successful gRPC PutWorkflow responses. This is evidence
for5b to authenticate and independently check, not authority to publish/activate.
Every registration enforces TTL3600000 and CEL `input.multica_run_id`; the worker
unwraps nested `input` and never accepts authored triggers. Public Axios adapter
denies every engine REST request, including the SDK constructor's tenant probe.

Internal product actions use POST `/api/workflow-runs/{run_id}/actions`, JSON
`ActionRequest` from the SDK contracts, and headers `Authorization: Bearer TOKEN`,
`X-Workspace-ID`, `X-Multica-Workflow-Generation`, `X-Multica-Workflow-Signature`.
Signature is lowercase hex HMAC-SHA256(TOKEN,
`POST\nPATH\nWORKSPACE\nGENERATION\nEXACT_BODY`). Server must authorize every request,
bind run/step/generation, compare signature, and atomically bind operation key to
operation/payload. Success is `{ result: JSONValue }`; mapped statuses are401,403,
404,409,410,429,503. Responses are size bounded and redacted. This route is a5b/6
contract; it is not implemented by this package.

Real opt-in proof (Docker plus exact patched engine receipts required):
`cd server && MULTICA_NODE_WORKER_LIVE=1 HATCHET_BIN_DIR=/tmp/multica-task5a-hatchet-bin GOPROXY=https://goproxy.cn,direct go test ./internal/hatchet -run '^TestPackagedNodeWorkerLive$' -count=1 -v`.
