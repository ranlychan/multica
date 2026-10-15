import { loadDefinition } from './load.js';
import { graphDigest, sourceHash } from './validate.js';
import type { ParentMessage, ChildMessage } from './contracts.js';
const send = (message: ChildMessage) => process.send?.(message);
process.once('message', async (message: ParentMessage) => {
  try {
    if (message.mode !== 'validate' && message.mode !== 'graph') throw new Error();
    const definition = await loadDefinition(message.source);
    send({ type: 'validated', result: { hash: sourceHash(message.source), graph: definition.graph, graph_digest: graphDigest(definition.graph) } });
  } catch (error) {
    const code = error instanceof Error && ['workflow_invalid_source', 'workflow_invalid_graph', 'workflow_invalid_import'].includes(error.message) ? error.message : 'workflow_invalid_source';
    send({ type: 'error', code });
  }
});
