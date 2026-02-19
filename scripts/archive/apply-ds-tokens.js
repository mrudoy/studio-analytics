const fs = require('fs');
const path = require('path');

const file = path.join(__dirname, '..', 'src', 'app', 'page.tsx');
let content = fs.readFileSync(file, 'utf8');
const lines = content.split('\n');

// Find excluded ranges
function findFunctionRange(lines, funcName) {
  let start = -1;
  for (let i = 0; i < lines.length; i++) {
    if (lines[i].includes(`function ${funcName}(`)) {
      start = i;
      break;
    }
  }
  if (start === -1) return null;
  // Find the end — next function at same indent or section separator
  let braceCount = 0;
  let foundFirst = false;
  for (let i = start; i < lines.length; i++) {
    for (const ch of lines[i]) {
      if (ch === '{') { braceCount++; foundFirst = true; }
      if (ch === '}') braceCount--;
    }
    if (foundFirst && braceCount === 0) return { start, end: i };
  }
  return { start, end: lines.length - 1 };
}

const pipelineRange = findFunctionRange(lines, 'PipelineView');
const freshnessRange = findFunctionRange(lines, 'FreshnessBadge');
const logoRange = findFunctionRange(lines, 'SkyTingLogo');

// DS constant definition range (lines 226-260 approximately)
const dsStart = lines.findIndex(l => l.includes('const DS = {'));
const dsEnd = dsStart >= 0 ? (() => {
  let bc = 0; let found = false;
  for (let i = dsStart; i < lines.length; i++) {
    for (const ch of lines[i]) { if (ch === '{') { bc++; found = true; } if (ch === '}') bc--; }
    if (found && bc === 0) return i;
  }
  return dsStart + 40;
})() : -1;

function isExcluded(lineNum) {
  if (pipelineRange && lineNum >= pipelineRange.start && lineNum <= pipelineRange.end) return true;
  if (freshnessRange && lineNum >= freshnessRange.start && lineNum <= freshnessRange.end) return true;
  if (logoRange && lineNum >= logoRange.start && lineNum <= logoRange.end) return true;
  if (dsStart >= 0 && lineNum >= dsStart && lineNum <= dsEnd) return true;
  return false;
}

// Check if line contains SVG attribute fontSize (fontSize="..." or fontSize={...} in JSX element attrs)
function isSvgFontSize(line) {
  // SVG uses fontFamily={FONT_SANS} fontSize="16" pattern (no colon before fontSize)
  return /fontFamily=\{FONT_SANS\}/.test(line) && /fontSize="?\d/.test(line);
}

let changes = 0;

for (let i = 0; i < lines.length; i++) {
  if (isExcluded(i)) continue;
  if (isSvgFontSize(lines[i])) continue;

  const orig = lines[i];
  let line = lines[i];

  // fontSize replacements (style object syntax: fontSize: "...")
  // xs: 0.6, 0.65, 0.68, 0.7, 0.72, 0.75, 0.78
  line = line.replace(/fontSize: "0\.6rem"/g, 'fontSize: DS.text.xs');
  line = line.replace(/fontSize: "0\.65rem"/g, 'fontSize: DS.text.xs');
  line = line.replace(/fontSize: "0\.68rem"/g, 'fontSize: DS.text.xs');
  line = line.replace(/fontSize: "0\.7rem"/g, 'fontSize: DS.text.xs');
  line = line.replace(/fontSize: "0\.72rem"/g, 'fontSize: DS.text.xs');
  line = line.replace(/fontSize: "0\.75rem"/g, 'fontSize: DS.text.xs');
  line = line.replace(/fontSize: "0\.78rem"/g, 'fontSize: DS.text.xs');

  // sm: 0.8, 0.82, 0.85, 0.88, 0.9, 0.92, 0.95
  line = line.replace(/fontSize: "0\.8rem"/g, 'fontSize: DS.text.sm');
  line = line.replace(/fontSize: "0\.82rem"/g, 'fontSize: DS.text.sm');
  line = line.replace(/fontSize: "0\.85rem"/g, 'fontSize: DS.text.sm');
  line = line.replace(/fontSize: "0\.88rem"/g, 'fontSize: DS.text.sm');
  line = line.replace(/fontSize: "0\.9rem"/g, 'fontSize: DS.text.sm');
  line = line.replace(/fontSize: "0\.92rem"/g, 'fontSize: DS.text.sm');
  line = line.replace(/fontSize: "0\.95rem"/g, 'fontSize: DS.text.sm');

  // md: 1, 1.15, 1.25
  line = line.replace(/fontSize: "1rem"/g, 'fontSize: DS.text.md');
  line = line.replace(/fontSize: "1\.15rem"/g, 'fontSize: DS.text.md');
  line = line.replace(/fontSize: "1\.25rem"/g, 'fontSize: DS.text.md');

  // lg: 1.3, 1.35, 1.4, 1.5, 1.8, 2
  line = line.replace(/fontSize: "1\.3rem"/g, 'fontSize: DS.text.lg');
  line = line.replace(/fontSize: "1\.35rem"/g, 'fontSize: DS.text.lg');
  line = line.replace(/fontSize: "1\.4rem"/g, 'fontSize: DS.text.lg');
  line = line.replace(/fontSize: "1\.5rem"/g, 'fontSize: DS.text.lg');
  line = line.replace(/fontSize: "1\.8rem"/g, 'fontSize: DS.text.lg');
  line = line.replace(/fontSize: "2rem"/g, 'fontSize: DS.text.lg');

  // xl: 2.4, 2.8
  line = line.replace(/fontSize: "2\.4rem"/g, 'fontSize: DS.text.xl');
  line = line.replace(/fontSize: "2\.8rem"/g, 'fontSize: DS.text.xl');

  // fontWeight replacements (in style objects)
  line = line.replace(/fontWeight: 400([,\s}])/g, 'fontWeight: DS.weight.normal$1');
  line = line.replace(/fontWeight: 500([,\s}])/g, 'fontWeight: DS.weight.normal$1');
  line = line.replace(/fontWeight: 600([,\s}])/g, 'fontWeight: DS.weight.medium$1');
  line = line.replace(/fontWeight: 700([,\s}])/g, 'fontWeight: DS.weight.bold$1');

  // letterSpacing normalization for uppercase labels
  line = line.replace(/letterSpacing: "0\.03em"/g, 'letterSpacing: "0.05em"');
  line = line.replace(/letterSpacing: "0\.04em"/g, 'letterSpacing: "0.05em"');
  line = line.replace(/letterSpacing: "0\.06em"/g, 'letterSpacing: "0.05em"');
  line = line.replace(/letterSpacing: "0\.08em"/g, 'letterSpacing: "0.05em"');

  // Card padding props
  line = line.replace(/<Card padding="1\.5rem">/g, '<Card>');
  line = line.replace(/<Card padding="1\.75rem">/g, '<Card>');

  if (line !== orig) changes++;
  lines[i] = line;
}

fs.writeFileSync(file, lines.join('\n'));
console.log(`Applied ${changes} line changes`);

// Count remaining hardcoded fontSizes outside excluded ranges
let remaining = 0;
const updatedLines = fs.readFileSync(file, 'utf8').split('\n');
for (let i = 0; i < updatedLines.length; i++) {
  if (isExcluded(i)) continue;
  if (isSvgFontSize(updatedLines[i])) continue;
  if (/fontSize: "[0-9]/.test(updatedLines[i])) {
    remaining++;
    console.log(`  Remaining L${i+1}: ${updatedLines[i].trim()}`);
  }
}
console.log(`Remaining hardcoded fontSizes: ${remaining}`);
