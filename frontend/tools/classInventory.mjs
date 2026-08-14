// Lists class names referenced from TSX that have no rule in styles.css, so the
// stylesheet rewrite cannot silently drop a component's styling.
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';

const root = path.resolve(process.cwd(), 'src');
const files = [];

async function walk(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) await walk(full);
    else if (/\.tsx$/.test(entry.name) && !/\.test\.tsx$/.test(entry.name)) files.push(full);
  }
}

await walk(root);

const used = new Map();
for (const file of files) {
  const source = await readFile(file, 'utf8');
  const short = path.relative(root, file);
  const matches = source.matchAll(/className=(?:"([^"]*)"|\{`([^`]*)`\}|\{'([^']*)'\})/g);
  for (const match of matches) {
    const raw = (match[1] ?? match[2] ?? match[3] ?? '')
      .replaceAll(/\$\{[^}]*\}/g, ' ')
      .replaceAll(/[?:'"]/g, ' ');
    for (const token of raw.split(/\s+/)) {
      if (!token || /[^a-z0-9-]/i.test(token)) continue;
      if (!used.has(token)) used.set(token, new Set());
      used.get(token).add(short);
    }
  }
}

const css = await readFile(path.resolve(process.cwd(), 'src/styles.css'), 'utf8');
const declared = new Set([...css.matchAll(/\.([a-z][a-z0-9-]*)/gi)].map((match) => match[1]));

const missing = [...used.entries()]
  .filter(([name]) => !declared.has(name))
  .sort(([a], [b]) => a.localeCompare(b));

for (const [name, sources] of missing) {
  console.log(`MISSING .${name}\t${[...sources].join(',')}`);
}
console.log(`\n${used.size} class tokens referenced, ${missing.length} without a CSS rule`);
