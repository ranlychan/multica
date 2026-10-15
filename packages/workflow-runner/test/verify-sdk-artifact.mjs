import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import ts from 'typescript';

const pin = '377de5d9642933b835990b1e2bea81d08f85c943';
const integrity = 'sha512-cM9esZxPaJjds4+Z2qm6g7BLiMxaNlDurlyOZsuuNGpFvvvDp3DrU0LnuubtvUXG8mRSW/vMTF04uXJapBo+ZA==';
const response = await fetch('https://registry.npmjs.org/@hatchet-dev/typescript-sdk/1.34.0-alpha.2');
assert(response.ok);
const metadata = await response.json();
assert.equal(metadata.dist.integrity, integrity);
const archive = Buffer.from(await (await fetch(metadata.dist.tarball)).arrayBuffer());
assert.equal(`sha512-${createHash('sha512').update(archive).digest('base64')}`, integrity);
const sourceRepo = process.argv[2];
assert(sourceRepo, 'pass read-only Hatchet object database checkout path');
for (const file of ['v1/client/client', 'v1/declaration', 'v1/client/worker/context', 'v1/client/worker/worker', 'clients/rest/api', 'clients/transport/node-transport', 'clients/dispatcher/action', 'v1/client/duration']) {
  let source = execFileSync('git', ['-C', sourceRepo, 'show', `${pin}:sdks/typescript/src/${file}.ts`], { encoding: 'utf8' });
  // The pinned publish command runs resolve-tspaths after tsc.
  source = source.replace(/(['"])(@hatchet\/|@util\/|@clients\/|@models\/|@hatchet-dev\/typescript-sdk\/)(.*?)\1/g, (_, quote, prefix, target) => {
    const base = { '@hatchet/': '', '@util/': 'util/', '@clients/': 'clients/', '@models/': 'models/', '@hatchet-dev/typescript-sdk/': '' }[prefix];
    let relative = path.posix.relative(path.posix.dirname(file), base + target);
    if (!relative.startsWith('.')) relative = './' + relative;
    return quote + relative + quote;
  });
  const installed = await readFile(new URL(`../node_modules/@hatchet-dev/typescript-sdk/${file}.js`, import.meta.url), 'utf8');
  assert.equal(execFileSync('tar', ['-xzOf', '-', `package/${file}.js`], { input: archive, encoding: 'utf8' }), installed);
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2016, esModuleInterop: true } }).outputText;
  const normalize = code => ts.createPrinter({ removeComments: true }).printFile(ts.createSourceFile('module.js', code, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS));
  assert.equal(normalize(installed), normalize(compiled), file);
  console.log(`${file}: registry bytes + normalized pinned emit MATCH`);
}
console.log(`SDK 1.34.0-alpha.2 archive integrity MATCH ${integrity}`);
