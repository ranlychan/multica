// @vitest-environment node
import { expect, it } from 'vitest';
import { actionNames, validateDescriptor, verifyRevision } from './worker.js';
import { sourceHash, graphDigest } from './validate.js';
it('matches Go logical task aliases and reconciler full action IDs', () => {
  expect(actionNames('multica-uuid-v1-0123456789ab', 'first')).toEqual({ task_name: 'multica-uuid-v1-0123456789ab-step-a7937b64b8caa58f', action_id: 'multica-uuid-v1-0123456789ab:multica-uuid-v1-0123456789ab-step-a7937b64b8caa58f' });
});
it('rejects wildcard dial hosts, plaintext and mismatched tenant subject', () => {
  const token = `e30.${Buffer.from(JSON.stringify({ sub: 'tenant' })).toString('base64url')}.fake`;
  const base = { protocol: 1, workspace_id: 'ws', tenant_id: 'tenant', generation: 'g', engine: { token, host_port: 'localhost:7070', tls: { strategy: 'mtls', server_name: 'cluster', ca_file: '/ca', cert_file: '/cert', key_file: '/key' } }, product: { origin: 'https://product.example', runner_token: 'private' }, revisions: [] };
  expect(() => validateDescriptor(base as never)).not.toThrow();
  for (const d of [{ ...base, tenant_id: 'other' }, { ...base, engine: { ...base.engine, host_port: '0.0.0.0:7070' } }, { ...base, engine: { ...base.engine, tls: { strategy: 'none' } } }]) expect(() => validateDescriptor(d as never)).toThrow('workflow_invalid_descriptor');
});
it('binds source and extracted graph before engine registration', async () => {
  const source = `import {defineWorkflow} from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'first',run:async()=>null}]});`;
  const graph = { steps: [{ id: 'first', parents: [] }] };
  const revision = { workflow_id: 'id', revision_id: 'rev', version: 1, hash: sourceHash(source), source_path: '/unused', engine_workflow_name: `multica-id-v1-${sourceHash(source).slice(0, 12)}`, graph, graph_digest: graphDigest(graph) };
  await expect(verifyRevision(source, revision)).resolves.toHaveProperty('steps');
  await expect(verifyRevision(`${source}\n`, revision)).rejects.toThrow('workflow_hash_mismatch');
  await expect(verifyRevision(source, { ...revision, graph: { steps: [{ id: 'other', parents: [] }] } })).rejects.toThrow('workflow_graph_mismatch');
});
