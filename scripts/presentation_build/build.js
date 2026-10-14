// Draws the scenes of deck.js as docs/presentation/FunnelStream-Presentation.pptx and .html.
//   cd scripts/presentation_build && npm install && node build.js
const fs = require("fs");
const path = require("path");
const pptxgen = require("pptxgenjs");
const { slides, C, FONT, ROOT } = require("./deck");

const OUT = path.join(ROOT, "docs", "presentation");
const W = 13.333, H = 7.5;

// ---- helpers shared by both renderers ---------------------------------------------------------------------------
const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
const paraRuns = (p) => p.runs || [{ text: p.text }];

// ---- PPTX -------------------------------------------------------------------------------------------------------
function buildPptx() {
  const pres = new pptxgen();
  pres.layout = "LAYOUT_WIDE";            // 13.33 x 7.5 in
  pres.title = "FunnelStream";
  pres.subject = "Project defense, 20 minutes";
  pres.author = "FunnelStream team";
  pres.theme = { headFontFace: FONT, bodyFontFace: FONT };
  slides.forEach((s, idx) => {
    const slide = pres.addSlide();
    slide.background = { color: s.bg };
    s.items.forEach((e, k) => drawPptx(pres, slide, e, `s${idx + 1}-${k}`));
    slide.addNotes(s.notes + (s.backup ? "" : `\n\nPlanned time: ${s.minutes}.`));
  });
  return pres.writeFile({ fileName: path.join(OUT, "FunnelStream-Presentation.pptx") });
}

function pptxParas(e) {
  const out = [];
  e.paras.forEach((p, i) => {
    const runs = paraRuns(p);
    runs.forEach((r, j) => {
      const o = {
        fontFace: e.font, fontSize: p.size || e.size, color: p.color || e.color, bold: r.bold != null ? r.bold : (p.bold != null ? p.bold : e.bold),
        italic: p.italic || e.italic || false, align: p.align || e.align,
      };
      if (j === 0 && p.bullet) o.bullet = { indent: 16 };
      if (j === runs.length - 1) {
        if (i < e.paras.length - 1) o.breakLine = true;
        if (p.after != null) o.paraSpaceAfter = p.after;
      }
      out.push({ text: r.text, options: o });
    });
  });
  return out;
}

function drawPptx(pres, slide, e, name) {
  if (e.t === "rect") {
    const opts = { x: e.x, y: e.y, w: e.w, h: e.h, fill: { color: e.fill }, line: e.line ? { color: e.line, width: 1 } : { color: e.fill, width: 0 }, objectName: name };
    if (e.radius > 0.05 && e.w > 0.3) { opts.rectRadius = Math.min(e.radius, e.h / 2, e.w / 2); slide.addShape(pres.ShapeType.roundRect, opts); }
    else slide.addShape(pres.ShapeType.rect, opts);
  } else if (e.t === "text") {
    slide.addText(pptxParas(e), { x: e.x, y: e.y, w: e.w, h: e.h, margin: 0, valign: e.valign, isTextBox: true, fit: "none", objectName: name, lineSpacingMultiple: 1.0 });
  } else if (e.t === "arrow") {
    const x = Math.min(e.x1, e.x2), y = Math.min(e.y1, e.y2);
    const w = Math.abs(e.x2 - e.x1), h = Math.abs(e.y2 - e.y1);
    slide.addShape(pres.ShapeType.line, { x, y, w, h, flipH: e.x2 < e.x1, flipV: e.y2 < e.y1, line: { color: e.color, width: e.width, endArrowType: "triangle" }, objectName: name });
  } else if (e.t === "img") {
    if (e.frame) slide.addShape(pres.ShapeType.rect, { x: e.x - 0.04, y: e.y - 0.04, w: e.w + 0.08, h: e.h + 0.08, fill: { color: C.white }, line: { color: C.line, width: 1 }, shadow: { type: "outer", color: "000000", opacity: 0.18, blur: 6, offset: 2, angle: 90 } });
    slide.addImage({ path: path.join(OUT, e.file), x: e.x, y: e.y, w: e.w, h: e.h, altText: path.basename(e.file, path.extname(e.file)).replace(/_/g, " ") + " screenshot" });
  } else if (e.t === "table") {
    const rows = e.rows.map((row, ri) => row.map((cell) => ({ text: cell, options: ri === 0
      ? { bold: true, color: C.white, fill: { color: C.navy }, fontFace: FONT, fontSize: e.size, valign: "middle" }
      : { color: C.ink, fill: { color: ri % 2 ? C.white : C.mist }, fontFace: FONT, fontSize: e.size, valign: "middle" } })));
    slide.addTable(rows, { x: e.x, y: e.y, colW: e.colW, rowH: e.rowH, border: { type: "solid", pt: 0.5, color: C.line }, margin: [0.03, 0.1, 0.03, 0.1] });
  }
}

// ---- HTML -------------------------------------------------------------------------------------------------------
const PX = 100;                              // 1 inch = 100 px on the stage, so 1 pt = 100/72 px
const pt = (v) => ((v * PX) / 72).toFixed(2) + "px";
const px = (v) => (v * PX).toFixed(1) + "px";

function htmlParas(e) {
  return e.paras.map((p) => {
    const runs = paraRuns(p).map((r) => {
      const bold = r.bold != null ? r.bold : (p.bold != null ? p.bold : e.bold);
      return `<span style="font-weight:${bold ? 700 : 400}">${esc(r.text)}</span>`;
    }).join("");
    const style = `font-size:${pt(p.size || e.size)};color:#${p.color || e.color};text-align:${p.align || e.align};` +
      `margin-bottom:${p.after != null ? pt(p.after) : "0"};${p.italic || e.italic ? "font-style:italic;" : ""}`;
    return `<p class="${p.bullet ? "bul" : ""}" style="${style}">${runs}</p>`;
  }).join("");
}

function drawHtml(e) {
  const step = e.step ? ` data-step="${e.step}"` : "";
  const cls = e.step ? " step" : "";
  if (e.t === "rect") {
    const r = e.radius > 0.05 && e.w > 0.3 ? Math.min(e.radius, e.h / 2, e.w / 2) : 0;
    return `<div class="el${cls}"${step} style="left:${px(e.x)};top:${px(e.y)};width:${px(e.w)};height:${px(e.h)};background:#${e.fill};` +
      `${e.line ? `border:1px solid #${e.line};` : ""}border-radius:${px(r)}"></div>`;
  }
  if (e.t === "text") {
    const jc = { top: "flex-start", middle: "center", bottom: "flex-end" }[e.valign];
    return `<div class="el tx${cls}"${step} style="left:${px(e.x)};top:${px(e.y)};width:${px(e.w)};height:${px(e.h)};justify-content:${jc};font-family:${e.font},'Segoe UI',Arial,sans-serif"><div>${htmlParas(e)}</div></div>`;
  }
  if (e.t === "img") {
    return `<img class="el${e.frame ? " frame" : ""}" src="${e.file}" alt="" style="left:${px(e.x)};top:${px(e.y)};width:${px(e.w)};height:${px(e.h)}">`;
  }
  if (e.t === "table") {
    const total = e.colW.reduce((a, c) => a + c, 0);
    const head = e.rows[0].map((c, i) => `<th style="width:${px(e.colW[i])}">${esc(c)}</th>`).join("");
    const body = e.rows.slice(1).map((r) => `<tr>${r.map((c) => `<td>${esc(c)}</td>`).join("")}</tr>`).join("");
    return `<table class="el" style="left:${px(e.x)};top:${px(e.y)};width:${px(total)};font-size:${pt(e.size)};--rh:${px(e.rowH)}"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  }
  return "";
}

function arrowSvg(arrows, sid) {
  let defs = "", body = "";
  const colors = [...new Set(arrows.map((a) => a.color))];
  colors.forEach((c) => { defs += `<marker id="m${sid}${c}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#${c}"/></marker>`; });
  arrows.forEach((a) => {
    const x1 = a.x1 * PX, y1 = a.y1 * PX, x2 = a.x2 * PX, y2 = a.y2 * PX;
    const step = a.step ? ` class="step" data-step="${a.step}"` : "";
    let g = `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="#${a.color}" stroke-width="${a.width * 1.4}" marker-end="url(#m${sid}${a.color})"/>`;
    if (a.flow) g += [0, 1].map((k) => `<circle r="${3 + a.width}" fill="#${a.color}" opacity="0.9"><animateMotion dur="${(1.6 + Math.hypot(x2 - x1, y2 - y1) / 400).toFixed(2)}s" begin="${k * 0.8}s" repeatCount="indefinite" path="M${x1} ${y1} L${x2} ${y2}"/></circle>`).join("");
    body += `<g${step}>${g}</g>`;
  });
  return `<svg class="el" viewBox="0 0 ${W * PX} ${H * PX}" style="left:0;top:0;width:${px(W)};height:${px(H)};pointer-events:none"><defs>${defs}</defs>${body}</svg>`;
}

function buildHtml() {
  const sections = slides.map((s, i) => {
    const arrows = s.items.filter((e) => e.t === "arrow");
    const rest = s.items.filter((e) => e.t !== "arrow");
    // arrows are drawn after the rectangles and before the text so text stays on top
    const rects = rest.filter((e) => e.t === "rect" || e.t === "img").map(drawHtml).join("");
    const others = rest.filter((e) => e.t !== "rect" && e.t !== "img").map(drawHtml).join("");
    return `<section class="slide${s.dark ? " dark" : ""}" data-steps="${s.steps || 0}" data-min="${esc(s.minutes)}" data-backup="${s.backup ? 1 : 0}" style="background:#${s.bg}">` +
      `${rects}${arrows.length ? arrowSvg(arrows, i) : ""}${others}<aside class="notes">${esc(s.notes).replace(/\n/g, "<br>")}</aside></section>`;
  }).join("\n");
  const css = fs.readFileSync(path.join(__dirname, "present.css"), "utf8");
  const js = fs.readFileSync(path.join(__dirname, "present.js"), "utf8");
  const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>FunnelStream: project presentation</title>
<style>${css}</style></head>
<body>
<div id="viewport"><div id="stage">
${sections}
</div></div>
<div id="hud"><span id="count"></span><span id="clock">00:00</span><span id="planned"></span></div>
<div id="notesPanel" hidden></div>
<div id="help" hidden>Right arrow, Space: next &nbsp; Left arrow: back &nbsp; O: overview &nbsp; N: speaker notes &nbsp; T: start or reset the 20-minute timer &nbsp; F: full screen &nbsp; H: hide this help</div>
<script>${js}</script></body></html>`;
  fs.writeFileSync(path.join(OUT, "FunnelStream-Presentation.html"), html, "utf8");
}

(async () => {
  fs.mkdirSync(OUT, { recursive: true });
  await buildPptx();
  buildHtml();
  const planned = slides.filter((s) => !s.backup).reduce((a, s) => { const [m, sec] = s.minutes.split(":").map(Number); return a + m * 60 + sec; }, 0);
  console.log(`slides: ${slides.length} (${slides.filter((s) => !s.backup).length} timed), planned time ${Math.floor(planned / 60)}:${String(planned % 60).padStart(2, "0")}`);
})();
