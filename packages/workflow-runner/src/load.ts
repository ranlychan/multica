import { build } from 'esbuild';
import { parse } from 'acorn';
import { defineWorkflow, limits, WorkflowError, type WorkflowDefinition } from '@multica/workflow-sdk';
/** Runs OS-user-trusted authored code. This is import policy, not a sandbox. */
export async function loadDefinition(source: string): Promise<WorkflowDefinition> {
  if (!source || Buffer.byteLength(source) > limits.sourceBytes) throw new WorkflowError('workflow_invalid_source');
  let invalidImport = false;
  let compiled;
  try {
    compiled = await build({ stdin: { contents: source, loader: 'ts', sourcefile: 'workflow.ts' }, tsconfigRaw: { compilerOptions: { verbatimModuleSyntax: true } }, bundle: true, write: false, platform: 'node', format: 'esm', target: 'node22', logLevel: 'silent', plugins: [{ name: 'workflow-imports', setup(builder) {
      builder.onResolve({ filter: /.*/ }, args => {
        if (args.path !== '@multica/workflow-sdk') { invalidImport = true; return { errors: [{ text: 'workflow_invalid_import' }] }; }
        return { path: import.meta.resolve('@multica/workflow-sdk'), external: true };
      });
    } }] });
  } catch { throw new WorkflowError(invalidImport ? 'workflow_invalid_import' : 'workflow_invalid_source'); }
  // esbuild leaves computed imports in deferred callbacks untouched. Inspect its
  // emitted JavaScript before evaluating any authored declaration.
  if (compiled.warnings.length || compiled.outputFiles.length !== 1) throw new WorkflowError('workflow_invalid_import');
  const code = Buffer.from(compiled.outputFiles[0]!.contents).toString('utf8');
  function check(node: unknown): void {
    if (!node || typeof node !== 'object') return;
    const value = node as Record<string, unknown>;
    if (value.type === 'ImportExpression') {
      const imported = value.source as { type?: string; value?: string };
      if (imported.type !== 'Literal' || imported.value !== import.meta.resolve('@multica/workflow-sdk')) throw new WorkflowError('workflow_invalid_import');
    }
    if (value.type === 'CallExpression' && ['require', '__require'].includes((value.callee as { name?: string })?.name ?? '')) throw new WorkflowError('workflow_invalid_import');
    for (const child of Object.values(value)) {
      if (Array.isArray(child)) child.forEach(check);
      else if (child && typeof child === 'object') check(child);
    }
  }
  check(parse(code, { ecmaVersion: 'latest', sourceType: 'module' }));
  try {
    const module = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputFiles[0]!.contents).toString('base64')}#${crypto.randomUUID()}`);
    const value = module.default;
    if (!value || !Array.isArray(value.steps) || Object.keys(value).some(k => !['steps', 'graph'].includes(k))) throw new WorkflowError('workflow_invalid_graph');
    return defineWorkflow({ steps: value.steps });
  } catch (error) {
    if (error instanceof WorkflowError && error.code === 'workflow_invalid_graph') throw error;
    throw new WorkflowError('workflow_invalid_source');
  }
}
