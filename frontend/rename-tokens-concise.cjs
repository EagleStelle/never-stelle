const fs = require('fs');
const path = require('path');

const map = {
  'ns-canvas-default': 'bg',
  'ns-surface-default': 'panel',
  'ns-surface-subtle': 'panel-subtle',
  'ns-foreground-default': 'text',
  'ns-foreground-muted': 'text-muted',
  'ns-foreground-inverse': 'text-inverse',
  'ns-accent-default': 'accent',
  'ns-accent-subtle': 'accent-subtle',
  'ns-separator-strong': 'border-strong', // Must be matched before separator-default
  'ns-separator-default': 'border'
};

function walk(dir) {
  let results = [];
  const list = fs.readdirSync(dir);
  list.forEach(function(file) {
    file = path.join(dir, file);
    const stat = fs.statSync(file);
    if (stat && stat.isDirectory()) {
      if (!file.includes('node_modules') && !file.includes('.git') && !file.includes('dist')) {
        results = results.concat(walk(file));
      }
    } else {
      if (file.endsWith('.vue') || file.endsWith('.ts') || file.endsWith('.html') || file.endsWith('.css')) {
        results.push(file);
      }
    }
  });
  return results;
}

const files = walk('c:/Users/solly/Documents/Website Development/never-stelle/frontend/src');
let changed = 0;

for (const file of files) {
  let content = fs.readFileSync(file, 'utf8');
  let newContent = content;

  for (const [oldKey, newKey] of Object.entries(map)) {
    const regex = new RegExp('\\b' + oldKey + '\\b', 'g');
    newContent = newContent.replace(regex, newKey);
  }

  if (content !== newContent) {
    fs.writeFileSync(file, newContent, 'utf8');
    changed++;
    console.log('Updated', file);
  }
}
console.log('Changed', changed, 'files');
