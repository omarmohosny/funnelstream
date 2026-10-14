// The FunnelStream presentation as data: one scene per slide, positions in inches on a 13.333 x 7.5 canvas.
// build.js draws the same scenes twice, as an editable .pptx and as an interactive .html, so both tell the same story.
// Every number below comes from docs/FunnelStream-Project-Documentation.md or from the screenshots in docs/images.
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..", "..");
const ASSETS = path.join(ROOT, "docs", "presentation", "assets");
const MAN = JSON.parse(fs.readFileSync(path.join(ASSETS, "manifest.json"), "utf8"));

const C = {
  navy: "0F3D6E", deep: "0B2F54", blue: "1F84FF", green: "59A472", greenDark: "2F7A4B", teal: "4AC5BB",
  sky: "D8E1EC", mist: "EEF2F7", ink: "121212", muted: "5B6578", white: "FFFFFF", coral: "BB4A4A", gold: "F4D25A",
  paleBlue: "A9CFFB", code: "0C1220", line: "C6D0DE",
};
const FONT = "Calibri";
const MONO = "Consolas";

// ---- element helpers --------------------------------------------------------------------------------------------
const el = {
  text(x, y, w, h, paras, o = {}) {
    if (typeof paras === "string") paras = [{ text: paras }];
    return Object.assign({ t: "text", x, y, w, h, paras, size: 16, color: C.ink, bold: false, align: "left", valign: "top", font: FONT }, o);
  },
  rect(x, y, w, h, o = {}) {
    return Object.assign({ t: "rect", x, y, w, h, fill: C.mist, line: null, radius: 0.1, shadow: false }, o);
  },
  arrow(x1, y1, x2, y2, o = {}) {
    return Object.assign({ t: "arrow", x1, y1, x2, y2, color: C.navy, width: 2, flow: false }, o);
  },
  img(name, x, y, w, h, o = {}) {
    const m = MAN[name];
    const scale = Math.min(w / m.w, h / m.h);
    const iw = m.w * scale, ih = m.h * scale;
    return Object.assign({ t: "img", file: m.file, x: x + (w - iw) / 2, y: y + (h - ih) / 2, w: iw, h: ih, frame: true }, o);
  },
  table(x, y, colW, rowH, rows, o = {}) {
    return Object.assign({ t: "table", x, y, colW, rowH, rows, size: 14 }, o);
  },
};

// A rounded card with a title and a body. Returns the elements.
function card(x, y, w, h, title, body, o = {}) {
  const fill = o.fill || C.mist;
  const dark = o.dark;
  const out = [el.rect(x, y, w, h, { fill, line: o.line || null, radius: 0.12, step: o.step })];
  const pad = o.pad == null ? 0.18 : o.pad;
  const tcol = o.titleColor || (dark ? C.white : C.navy);
  const bcol = o.bodyColor || (dark ? "DCE6F2" : C.ink);
  const paras = [{ text: title, size: o.titleSize || 17, bold: true, color: tcol, after: 4 }];
  if (Array.isArray(body)) body.forEach((b) => paras.push(typeof b === "string" ? { text: b, size: o.bodySize || 14, color: bcol, after: 3 } : b));
  else if (body) paras.push({ text: body, size: o.bodySize || 14, color: bcol });
  out.push(el.text(x + pad, y + pad * 0.8, w - 2 * pad, h - 1.6 * pad, paras, { step: o.step }));
  return out;
}

function chrome(n, section, title, opts = {}) {
  const dark = opts.dark;
  const items = [];
  if (section) items.push(el.text(0.6, 0.3, 9, 0.3, section, { size: 12, bold: true, color: dark ? C.paleBlue : C.muted }));
  items.push(el.text(0.6, 0.58, 12.1, 0.75, title, { size: opts.titleSize || 30, bold: true, color: dark ? C.white : C.navy, valign: "middle" }));
  if (!dark) {
    items.push(el.text(0.6, 7.04, 6, 0.3, "FunnelStream", { size: 10, color: C.muted }));
    items.push(el.text(11.9, 7.04, 0.83, 0.3, String(n), { size: 10, color: C.muted, align: "right" }));
  }
  return items;
}

// A screenshot slide: the image large on one side, short commentary on the other.
function shotSlide(n, section, title, image, caption, heading, bullets, o = {}) {
  const imgBox = o.imgBox || [0.6, 1.5, 7.9, 5.1];
  const items = chrome(n, section, title);
  items.push(el.img(image, ...imgBox));
  const tx = imgBox[0] + imgBox[2] + 0.35, tw = 13.333 - 0.6 - tx;
  const paras = [{ text: heading, size: 18, bold: true, color: C.navy, after: 8 }];
  bullets.forEach((b) => paras.push({ runs: b, size: 15, bullet: true, after: 7 }));
  items.push(el.text(tx, imgBox[1], tw, imgBox[3] + 0.1, paras));
  items.push(el.text(imgBox[0], 6.62, imgBox[2], 0.4, caption, { size: 11, color: C.muted, italic: true, align: "center" }));
  return items;
}
const b = (text) => ({ text, bold: true });
const r = (text) => ({ text });

const slides = [];

// ============================== 1. Title ==============================================================================
{
  const items = [];
  items.push(el.text(0.8, 1.55, 7.2, 1.1, "FunnelStream", { size: 60, bold: true, color: C.white, valign: "middle" }));
  items.push(el.text(0.8, 2.75, 6.7, 1.6, "Real-time e-commerce and campaign analytics: from the first click to the next morning's report", { size: 22, color: "DCE6F2" }));
  items.push(el.text(0.8, 4.6, 6.7, 0.4, "Kafka  |  Spark  |  Delta  |  Databricks  |  Postgres  |  Grafana  |  Power BI", { size: 14, color: C.paleBlue }));
  items.push(el.text(0.8, 5.9, 6.7, 0.9, [{ text: "The FunnelStream team", size: 18, bold: true, color: C.white, after: 4 },
    { text: "Project defense, 20 minutes", size: 14, color: "DCE6F2" }]));
  // the funnel of the Funnel page of the Power BI report, drawn from its real numbers (simulated data)
  const stages = [["Sessions", "16,617", 5.0], ["Product views", "16,120", 4.85], ["Cart", "5,578", 3.2], ["Checkout", "2,486", 2.2], ["Orders", "1,723", 1.6]];
  const cx = 10.15, top = 1.45;
  const cols = [C.paleBlue, "6FB0FA", C.blue, "186DD5", C.green];
  stages.forEach((s, i) => {
    const y = top + i * 0.92;
    items.push(el.rect(cx - s[2] / 2, y, s[2], 0.78, { fill: cols[i], radius: 0.1 }));
    items.push(el.text(cx - s[2] / 2, y, s[2], 0.78, [{ text: s[1], size: 22, bold: true, color: i < 2 ? C.deep : C.white, align: "center" },
      { text: s[0], size: 12, color: i < 2 ? C.deep : C.white, align: "center" }], { valign: "middle", align: "center" }));
  });
  items.push(el.text(7.7, 6.2, 5.1, 0.6, "Funnel page of the Power BI report. The data is simulated: 16,617 sessions end in 1,723 orders (10.4%).", { size: 11, color: C.paleBlue, italic: true, align: "center" }));
  slides.push({ bg: C.deep, dark: true, items, minutes: "0:45",
    notes: "[0:45] Good morning. We are the FunnelStream team. FunnelStream is a data platform for an online store: it takes every click, order, delivery, ad number and stock movement, and turns them into two things: a live view for the people who run the store today, and a trusted report for the people who spend the advertising money. The funnel on the right is a real page of our report: 16,617 sessions end in 1,723 orders. The data is simulated, and I will say so whenever it matters. In 20 minutes I will show the business problem, the architecture, how we keep the data clean, and the two dashboards." });
}

// ============================== 2. Business problem ===================================================================
{
  const items = chrome(2, "1  BUSINESS CONTEXT", "Three questions a store cannot answer from one place");
  const cw = 3.9, gap = 0.2, y = 1.75, h = 3.35;
  const data = [
    ["1", "Which campaign really pays?", "Ads are paid per click, but cash-on-delivery orders are often cancelled or fail delivery. Return on ad spend measured on placed orders overstates the result."],
    ["2", "Where do shoppers drop out?", "View, cart, checkout, order. Which step, on which channel and platform, should be fixed first?"],
    ["3", "What needs action right now?", "Which products and dark stores are out of stock, is the 20-minute express promise holding, and is advertising still sending shoppers to empty shelves?"],
  ];
  data.forEach((d, i) => {
    const x = 0.6 + i * (cw + gap);
    items.push(el.rect(x, y, cw, h, { fill: C.mist, radius: 0.12 }));
    items.push(el.rect(x + 0.22, y + 0.22, 0.6, 0.6, { fill: C.navy, radius: 0.3 }));
    items.push(el.text(x + 0.22, y + 0.22, 0.6, 0.6, d[0], { size: 22, bold: true, color: C.white, align: "center", valign: "middle" }));
    items.push(el.text(x + 0.22, y + 1.0, cw - 0.44, h - 1.15, [
      { text: d[1], size: 19, bold: true, color: C.navy, after: 6 }, { text: d[2], size: 15 }]));
  });
  items.push(el.rect(0.6, 5.35, 12.1, 1.35, { fill: C.navy, radius: 0.12 }));
  items.push(el.text(0.85, 5.45, 5.6, 1.15, [{ text: "Operations user", size: 17, bold: true, color: C.white, after: 3 },
    { text: "Acts within a minute. Needs live traffic, real stock and the express promise.", size: 14, color: "DCE6F2" }], { valign: "middle" }));
  items.push(el.text(6.85, 5.45, 5.6, 1.15, [{ text: "Marketing user", size: 17, bold: true, color: C.white, after: 3 },
    { text: "Reads yesterday each morning. Needs trusted campaign, funnel and sales numbers.", size: 14, color: "DCE6F2" }], { valign: "middle" }));
  slides.push({ bg: C.white, items, minutes: "1:15",
    notes: "[1:15] An online store in Egypt spends money on Meta and Google ads, ships mostly cash on delivery, and in the quick-commerce case promises delivery from a dark store in 20 minutes. Three questions are hard to answer in one place. First: which campaign really pays. Cash-on-delivery orders are often cancelled or fail delivery, so return measured on placed orders flatters the campaign. Second: where shoppers drop out between view, cart, checkout and order. Third: what needs action right now, which products are out of stock and whether ads are still sending shoppers to empty shelves. Two different people ask these questions: an operations user who must act within a minute, and a marketing user who reads yesterday's results." });
}

// ============================== 3. Solution ===========================================================================
{
  const items = chrome(3, "1  BUSINESS CONTEXT", "One stream of events, two lanes, two users");
  items.push(...card(0.6, 1.9, 2.7, 3.7, "The online store", ["Customers browse, add to cart, check out", "Orders are packed, shipped, delivered, cancelled or returned", "Eight dark stores hold stock", "Campaigns bring the traffic", { text: "All simulated", size: 13, color: C.muted, bold: true }], { fill: C.mist, bodySize: 14 }));
  items.push(el.arrow(3.3, 3.75, 3.85, 3.75, { color: C.navy, width: 2.5 }));
  items.push(...card(3.85, 2.35, 3.0, 2.8, "Kafka, Bronze, Silver", ["Every event is kept raw.", "Then checked, typed and deduplicated once.", "One trusted copy feeds both lanes."], { fill: C.navy, dark: true, bodySize: 14, step: 1 }));
  items.push(el.arrow(6.85, 3.2, 7.45, 2.9, { color: C.green, width: 2.5, step: 2 }));
  items.push(el.arrow(6.85, 4.3, 7.45, 4.9, { color: C.blue, width: 2.5, step: 3 }));
  items.push(...card(7.45, 1.9, 5.25, 2.0, "Live lane: seconds to a minute", ["Postgres and Grafana, 8 alert rules.", "For operations: which product is out of stock, is express on time, is the pipeline healthy."], { fill: "E3F1E8", titleColor: C.greenDark, bodySize: 14, step: 2 }));
  items.push(...card(7.45, 4.1, 5.25, 2.0, "Batch lane: next morning", ["Databricks Gold with 66 quality checks, Power BI.", "For marketing: spend, ROAS, funnel, customers, delivered revenue."], { fill: "E1ECFB", titleColor: C.navy, bodySize: 14, step: 3 }));
  items.push(el.rect(0.6, 6.3, 12.1, 0.6, { fill: C.gold, radius: 0.1, step: 4 }));
  items.push(el.text(0.6, 6.3, 12.1, 0.6, "The bridge only a joined platform can make: advertising spend sent to products that were out of stock", { size: 16, bold: true, color: C.ink, align: "center", valign: "middle", step: 4 }));
  slides.push({ bg: C.white, items, minutes: "1:15", steps: 4,
    notes: "[1:15] FunnelStream's answer is one stream of events and two lanes. Everything the store does becomes an event in Kafka. We keep every raw message, then clean it once into a trusted copy called Silver. From Silver, a live lane writes small tables to Postgres within a minute and Grafana shows them with eight alert rules: that is for operations. A batch lane publishes Silver to Databricks, builds a Gold star schema with 66 data quality checks, and feeds a seven-page Power BI report: that is for marketing. The yellow bar is the point of the whole project: because both lanes come from the same events, we can tell marketing how much advertising spend landed on products that were out of stock." });
}

// ============================== 4. Architecture =======================================================================
{
  const items = chrome(4, "2  ARCHITECTURE AND DATA ENGINEERING", "End-to-end architecture");
  const box = (x, y, w, h, title, sub, o = {}) => [el.rect(x, y, w, h, { fill: o.fill || C.mist, line: o.line || C.line, radius: 0.1, step: o.step }),
    el.text(x + 0.1, y + 0.08, w - 0.2, h - 0.16, [{ text: title, size: 15, bold: true, color: o.tc || C.navy, align: "center", after: 2 },
      { text: sub, size: 12, color: o.sc || C.ink, align: "center" }], { valign: "middle", align: "center", step: o.step })];
  items.push(...box(0.5, 1.95, 2.2, 1.35, "Simulator", "app events, orders, deliveries, stock of 8 dark stores", { step: 1 }));
  items.push(...box(0.5, 4.9, 2.2, 1.0, "Mock ad API", "Meta and Google campaigns", { step: 1 }));
  items.push(...box(3.2, 1.95, 2.0, 1.35, "Kafka", "4 topics, KRaft mode", { fill: C.blue, line: C.blue, tc: C.white, sc: C.white, step: 1 }));
  items.push(...box(3.2, 4.9, 2.0, 1.0, "Airflow", "daily ad report, 03:00 UTC", { step: 1 }));
  items.push(el.rect(5.7, 1.75, 3.1, 4.35, { fill: "F7F9FC", line: C.navy, radius: 0.12, step: 2 }));
  items.push(el.text(5.8, 1.8, 2.9, 0.3, "Spark Structured Streaming, in Docker", { size: 11, bold: true, color: C.navy, align: "center", step: 2 }));
  items.push(...box(5.9, 2.2, 2.7, 0.9, "Bronze", "raw messages, append-only", { fill: "FDE9D9", line: "E7B58F", step: 2 }));
  items.push(...box(5.9, 3.55, 2.7, 1.0, "Silver", "validated, deduplicated, Delta MERGE", { fill: "E3E8EE", line: "9AA7B8", step: 2 }));
  items.push(...box(5.9, 4.95, 2.7, 0.85, "Quarantine", "rejected rows, with a reason", { fill: "F9E0E0", line: "E0A3A3", step: 2 }));
  items.push(el.text(9.3, 1.45, 3.6, 0.3, "LIVE LANE  |  within a minute", { size: 11, bold: true, color: C.greenDark, step: 3 }));
  items.push(...box(9.3, 1.85, 1.75, 1.45, "Live sink + Postgres", "16 live and serving tables", { fill: "E3F1E8", line: "9CCBAB", step: 3 }));
  items.push(...box(11.4, 1.85, 1.5, 1.45, "Grafana", "6 dashboards, 8 alerts", { fill: C.green, line: C.green, tc: C.white, sc: C.white, step: 3 }));
  items.push(el.text(9.3, 3.8, 3.6, 0.3, "BATCH LANE  |  next morning", { size: 11, bold: true, color: C.navy, step: 4 }));
  items.push(...box(9.3, 4.2, 1.75, 1.45, "Databricks", "Gold star schema, 66 checks", { fill: "E1ECFB", line: "9DB9E6", step: 4 }));
  items.push(...box(11.4, 4.2, 1.5, 1.45, "Power BI", "7 pages, 103 measures", { fill: C.navy, line: C.navy, tc: C.white, sc: C.white, step: 4 }));
  items.push(...box(9.3, 5.85, 3.6, 0.65, "Daily marts in Postgres", "inventory, express, D-1 marketing", { fill: "E3F1E8", line: "9CCBAB", step: 4 }));
  items.push(el.arrow(2.7, 2.62, 3.2, 2.62, { flow: true, step: 1 }));
  items.push(el.arrow(5.2, 2.65, 5.9, 2.65, { flow: true, step: 2 }));
  items.push(el.arrow(7.25, 3.1, 7.25, 3.55, { flow: true, step: 2 }));
  items.push(el.arrow(7.25, 4.55, 7.25, 4.95, { flow: true, step: 2, color: C.coral }));
  items.push(el.arrow(2.7, 5.4, 3.2, 5.4, { flow: true, step: 1 }));
  items.push(el.arrow(5.2, 5.15, 5.9, 4.2, { flow: true, step: 2 }));
  items.push(el.arrow(8.6, 3.75, 9.3, 2.9, { flow: true, color: C.green, step: 3 }));
  items.push(el.arrow(8.6, 4.35, 9.3, 4.8, { flow: true, color: C.blue, step: 4 }));
  items.push(el.arrow(11.05, 2.58, 11.4, 2.58, { flow: true, color: C.green, step: 3 }));
  items.push(el.arrow(11.05, 4.92, 11.4, 4.92, { flow: true, color: C.blue, step: 4 }));
  items.push(el.arrow(12.15, 5.85, 12.15, 5.65, { color: C.navy, step: 4 }));
  items.push(el.text(0.5, 6.62, 12.4, 0.35, "Everything left of Databricks runs on one Windows laptop in Docker. Gold is written only by the Databricks build.", { size: 12, color: C.muted, italic: true }));
  slides.push({ bg: C.white, items, minutes: "1:30", steps: 4,
    notes: "[1:30] This is the whole system on one slide. On the left, the simulator produces app events, orders, deliveries and stock for eight dark stores, and a mock ad API stands in for Meta and Google. Events go into Kafka. A Spark Structured Streaming job in Docker writes Bronze, the raw copy, then Silver, the validated and deduplicated copy; rows that fail validation go to a quarantine table with a reason. From Silver the system splits. The live lane, in green, recomputes small tables in Postgres after each batch and Grafana reads them. The batch lane, in blue, publishes Silver to Databricks, where the Gold star schema is built and checked by 66 SQL checks; Power BI reads Gold and the daily marts. Airflow pulls the ad report once a day. Everything left of Databricks runs on one laptop in Docker." });
}

// ============================== 5. Data sources and ingestion =========================================================
{
  const items = chrome(5, "2  ARCHITECTURE AND DATA ENGINEERING", "Data sources and ingestion: a seeded, noisy store simulator");
  items.push(...card(0.6, 1.75, 3.0, 1.75, "App and web events", "Product view, cart, checkout, purchase. Anonymous sessions are keyed by session_id.", { bodySize: 14 }));
  items.push(...card(3.8, 1.75, 3.0, 1.75, "Orders and deliveries", "Created only after checkout. Full lifecycle with cancel, return and failed delivery.", { bodySize: 14 }));
  items.push(...card(0.6, 3.7, 3.0, 1.75, "Stock events", "Snapshots and movements for 8 dark stores. Stock never goes below zero.", { bodySize: 14 }));
  items.push(...card(3.8, 3.7, 3.0, 1.75, "Mock ad API", "Meta and Google campaigns, pulled daily by Airflow into Silver.", { bodySize: 14 }));
  items.push(...card(0.6, 5.65, 6.2, 1.2, "Egyptian realism", "EGP, cash on delivery, Fawry, Vodafone Cash, InstaPay; Bosta, Aramex, J&T; Ramadan and Eid peaks; business day in Cairo time.", { fill: C.navy, dark: true, bodySize: 13, titleSize: 15 }));
  items.push(el.rect(7.1, 1.75, 5.6, 2.55, { fill: C.code, radius: 0.12 }));
  items.push(el.text(7.3, 1.85, 5.2, 0.3, "One envelope for every event", { size: 12, bold: true, color: C.paleBlue, font: FONT }));
  items.push(el.text(7.3, 2.2, 5.2, 2.0, [
    { text: "{", size: 14, color: "DCE6F2" }, { text: '  "event_id": ...,', size: 14, color: "DCE6F2" }, { text: '  "event_type": ...,', size: 14, color: "DCE6F2" },
    { text: '  "event_version": ...,', size: 14, color: "DCE6F2" }, { text: '  "event_time": ...,', size: 14, color: "DCE6F2" }, { text: '  "payload": { ... }', size: 14, color: "DCE6F2" }, { text: "}", size: 14, color: "DCE6F2" }], { font: MONO }));
  items.push(...card(7.1, 4.5, 5.6, 2.35, "Noise on purpose, seeded and counted", [
    "1% duplicate events, 0.3% malformed JSON, 2% late events, 3% dirty city text, bots, out-of-order order status.",
    { text: "A noise manifest records what was injected. After Silver, the quarantine count must match it.", size: 14, bold: true, color: C.navy }], { fill: "FDF3D6", bodySize: 13, titleSize: 16 }));
  slides.push({ bg: C.white, items, minutes: "1:15",
    notes: "[1:15] Data starts in a seeded simulator, so the same seed gives the same customers and events and every demo is repeatable. It produces app and web events with a realistic drop-off at each funnel step, orders and deliveries with a full lifecycle, stock events for eight dark stores, and campaign numbers from a mock ad API. We made it Egyptian on purpose: pounds, cash on delivery, Fawry and InstaPay, Bosta and Aramex, Ramadan and Eid peaks. Every event has the same envelope: an id, a type, a version, a time and a payload. And the simulator is deliberately dirty: duplicates, malformed JSON, late events, dirty text. It writes down how much noise it injected, so later the pipeline can prove it caught all of it." });
}

// ============================== 6. Streaming and messaging ============================================================
{
  const items = chrome(6, "2  ARCHITECTURE AND DATA ENGINEERING", "Streaming: Kafka decouples producers from processing");
  items.push(...card(0.6, 1.8, 4.7, 1.25, "Producers", "The simulator. acks=all and idempotent, so a retry never writes twice.", { bodySize: 14, titleSize: 16 }));
  items.push(el.arrow(2.95, 3.05, 2.95, 3.55, { flow: true, color: C.blue, width: 3 }));
  items.push(...card(0.6, 3.55, 4.7, 1.25, "Kafka 3.7, KRaft, 4 topics", "Keeps messages while the job is down. Replays after a crash.", { fill: C.blue, dark: true, bodySize: 14, titleSize: 16 }));
  items.push(el.arrow(2.95, 4.8, 2.95, 5.3, { flow: true, color: C.blue, width: 3 }));
  items.push(...card(0.6, 5.3, 4.7, 1.45, "Consumer: the Spark job", "Reads each topic in micro-batches. Its checkpoints hold the committed offsets, so a restart resumes where it stopped.", { bodySize: 14, titleSize: 16 }));
  items.push(el.table(5.7, 1.8, [1.8, 2.7, 1.35, 1.15], 0.58, [
    ["Topic", "Carries", "Key", "Partitions"],
    ["app.events", "10 app and web event types", "session_id", "6"],
    ["orders", "7 order status events", "order_id", "3"],
    ["delivery.events", "3 delivery events", "order_id", "3"],
    ["inventory.events", "stock snapshots, movements", "store_id", "3"],
  ], { size: 14 }));
  items.push(el.text(5.7, 4.85, 7.0, 2.0, [
    { runs: [b("Keys keep order. "), r("All events of a session, or of an order, go to one partition in sequence.")], size: 15, bullet: true, after: 8 },
    { runs: [b("Producers and the lake never talk to each other. "), r("Either side can fail or be replaced.")], size: 15, bullet: true, after: 8 },
    { runs: [b("Replay is safe. "), r("Silver writes are idempotent, so reading a message twice changes nothing.")], size: 15, bullet: true }]));
  slides.push({ bg: C.white, items, minutes: "1:15",
    notes: "[1:15] Kafka sits between the producers and everything else. Four topics: app.events with six partitions keyed by session, orders and delivery.events keyed by order id, and inventory.events keyed by store. The key matters: all events of one order land in one partition and stay in order. Our producer uses acks equals all and idempotence, so a retry does not write twice. The consumer is a Spark job that reads in micro-batches and stores the committed offsets in checkpoints. That is the point of the messaging layer: if the Spark job is killed, Kafka keeps the messages, and the restarted job resumes from its checkpoint. In the HTML version the dots show the data flowing." });
}

// ============================== 7. Processing =========================================================================
{
  const items = chrome(7, "2  ARCHITECTURE AND DATA ENGINEERING", "Processing: from a raw message to a trusted row");
  items.push(el.text(0.6, 1.5, 12.1, 0.4, "One event, end to end: an order_created message on the orders topic", { size: 16, color: C.muted, italic: true }));
  const steps = [
    ["1", "Kafka", "A new order. Topic orders, key order_id.", C.blue],
    ["2", "Bronze", "Raw value plus partition, offset and ingest time. Nothing changed, nothing dropped.", "C77B3B"],
    ["3", "Validate", "Schema contract per event type. A bad row goes to quarantine with an error code.", C.coral],
    ["4", "Silver", "Typed, UTC, deduplicated on event_id, written with a Delta MERGE. Status by rank.", "5B6B80"],
    ["5", "Consumers", "Live sink: Postgres within a minute. Publish: Databricks, then Gold.", C.green],
  ];
  steps.forEach((s, i) => {
    const x = 0.6 + i * 2.45, w = 2.25;
    items.push(el.rect(x, 2.05, w, 2.75, { fill: C.mist, radius: 0.12, step: i + 1 }));
    items.push(el.rect(x + 0.18, 2.23, 0.5, 0.5, { fill: s[3], radius: 0.25, step: i + 1 }));
    items.push(el.text(x + 0.18, 2.23, 0.5, 0.5, s[0], { size: 18, bold: true, color: C.white, align: "center", valign: "middle", step: i + 1 }));
    items.push(el.text(x + 0.18, 2.85, w - 0.36, 1.9, [{ text: s[1], size: 17, bold: true, color: C.navy, after: 3 }, { text: s[2], size: 14 }], { step: i + 1 }));
    if (i < 4) items.push(el.arrow(x + w, 3.4, x + w + 0.2, 3.4, { color: C.navy, width: 2.5, step: i + 2 }));
  });
  items.push(el.text(0.6, 5.0, 12.1, 0.3, "Rejected rows go to quarantine with one of these reasons", { size: 14, bold: true, color: C.navy }));
  const chips = [["BAD_JSON", 1.35], ["MISSING_FIELD", 1.7], ["UNKNOWN_EVENT_TYPE", 2.3], ["BAD_VERSION", 1.4], ["NEGATIVE_PRICE", 1.65], ["FUTURE_TIMESTAMP", 1.85], ["UNKNOWN_PRODUCT", 1.7]];
  let cx0 = 0.6; const cgap = (12.1 - chips.reduce((a, c) => a + c[1], 0)) / 6;
  chips.forEach((c) => {
    items.push(el.rect(cx0, 5.4, c[1], 0.4, { fill: "F9E0E0", radius: 0.2 }));
    items.push(el.text(cx0, 5.4, c[1], 0.4, c[0], { size: 11, bold: true, color: C.coral, align: "center", valign: "middle", font: MONO }));
    cx0 += c[1] + cgap;
  });
  items.push(el.rect(0.6, 6.0, 12.1, 0.8, { fill: "E3F1E8", radius: 0.1 }));
  items.push(el.text(0.8, 6.0, 11.7, 0.8, [{ runs: [b("Evidence, 13 October 2026: "), r("quarantine count matched the noise manifest (308 expected, 308 observed). Replaying Bronze left all 10 Silver tables unchanged.")], size: 14 }], { valign: "middle" }));
  slides.push({ bg: C.white, items, minutes: "1:30", steps: 5,
    notes: "[1:30] Follow one message: a new order. It arrives on the orders topic with the order id as key. Bronze stores the raw value plus the Kafka partition, offset and ingest time, and never changes it. Then the validator checks it against a versioned schema. If the JSON is broken, a field is missing, the price is negative, the timestamp is in the future or the product is unknown, the row goes to quarantine with an error code and never reaches the numbers. If it passes, Silver types it, converts the time to UTC, deduplicates on the event id and writes it with a Delta MERGE on the key, so a replay inserts nothing. Order status is decided by rank, not by arrival order. Two checks prove it: the quarantine count equals the noise the simulator injected, 308 for 308, and replaying Bronze left all ten Silver tables unchanged." });
}

// ============================== 8. Storage and layers =================================================================
{
  const items = chrome(8, "2  ARCHITECTURE AND DATA ENGINEERING", "Storage: three layers, three places");
  const row = (y, name, where, tables, fill, line, tc) => {
    items.push(el.rect(0.6, y, 12.1, 1.3, { fill, line, radius: 0.12 }));
    items.push(el.text(0.8, y + 0.1, 2.3, 1.1, [{ text: name, size: 20, bold: true, color: tc, after: 2 }, { text: where, size: 12, color: C.muted }], { valign: "middle" }));
    tables.forEach((t, i) => {
      const w = 12.1 - 2.7 - 0.2, cw = (w - 0.15 * (tables.length - 1)) / tables.length;
      const x = 3.2 + i * (cw + 0.15);
      items.push(el.rect(x, y + 0.2, cw, 0.9, { fill: C.white, line: C.line, radius: 0.08 }));
      items.push(el.text(x + 0.08, y + 0.2, cw - 0.16, 0.9, [{ text: t[0], size: 13, bold: true, color: tc, align: "center" }, { text: t[1], size: 11, color: C.muted, align: "center" }], { valign: "middle", align: "center" }));
    });
  };
  row(1.75, "Bronze", "raw, never changed", [["bronze_app_events", "6 partitions of raw messages"], ["bronze_orders", "order status messages"], ["bronze_delivery_events", "delivery messages"], ["bronze_inventory_events", "stock messages"]], "FDF1E6", "E7B58F", "9A5521");
  row(3.2, "Silver", "trusted, one row per key", [["silver_events", "one app event"], ["silver_orders, _status, _items", "order, status change, line"], ["silver_stock_*, _express_orders", "stock and express"], ["silver_quarantine", "rejected rows"]], "EDF0F4", "9AA7B8", "3D4B5E");
  row(4.65, "Gold", "business ready, Databricks", [["fact_orders, _lines, _attribution", "last and first touch"], ["dim_customer, dim_product", "slowly changing (SCD2)"], ["agg_orders, funnel, campaign, city_product", "daily aggregates"], ["customer_360", "one row per customer"]], "E5EFFC", "9DB9E6", C.navy);
  items.push(el.text(0.6, 6.15, 4.0, 0.8, [{ text: "Local lake", size: 15, bold: true, color: C.navy }, { text: "Bronze and Silver: Delta folders on disk, rebuildable", size: 13 }]));
  items.push(el.text(4.7, 6.15, 4.1, 0.8, [{ text: "Databricks, Unity Catalog", size: 15, bold: true, color: C.navy }, { text: "Silver mirror and 12 Gold tables", size: 13 }]));
  items.push(el.text(8.9, 6.15, 3.8, 0.8, [{ text: "Postgres 16", size: 15, bold: true, color: C.navy }, { text: "16 live and serving tables, daily marts", size: 13 }]));
  slides.push({ bg: C.white, items, minutes: "1:15",
    notes: "[1:15] Data lives in three layers. Bronze keeps every raw message and is never changed. Silver is trusted: one row per key, typed, deduplicated, with a quarantine table for rejected rows. Both run as Delta folders on the laptop, and Silver can be rebuilt from Bronze at any time. Gold is business ready and lives in Databricks: facts for orders, order lines and attribution, slowly changing dimensions for customers and products, daily aggregates for orders, funnel, campaigns and city by product, and a customer 360 table. Twelve Gold tables are built; a thirteenth slot for recommendations is empty on purpose. Operational data for Grafana lives in Postgres: sixteen small live and serving tables, plus the daily marts that Power BI also reads." });
}

// ============================== 9. Orchestration and reliability ======================================================
{
  const items = chrome(9, "2  ARCHITECTURE AND DATA ENGINEERING", "Reliability: crash, duplicates, late and bad data");
  items.push(el.table(0.6, 1.75, [2.7, 4.7], 0.66, [
    ["When this happens", "this protects the data"],
    ["The Spark job is killed", "Checkpoints hold the Kafka offsets. The restart resumes from them."],
    ["A message arrives twice", "Silver MERGE on the key inserts nothing the second time."],
    ["An event arrives late", "Flagged is_late. Left out of live metrics, corrected by the batch."],
    ["A row is bad", "Quarantined with a reason. Never in the numbers."],
    ["Databricks is unavailable", "A second Gold store backend loads the tables into Postgres."],
  ], { size: 14 }));
  items.push(...card(8.7, 1.75, 4.0, 2.2, "Airflow: ads_yesterday_report", ["Runs at 03:00 UTC. Pulls the ad API once for yesterday and stores a snapshot. Retries on HTTP 503. A rerun returns the stored report without calling the API."], { fill: C.mist, bodySize: 14, titleSize: 16 }));
  items.push(el.rect(8.7, 4.15, 4.0, 2.65, { fill: C.navy, radius: 0.12 }));
  items.push(el.text(8.9, 4.25, 3.6, 2.5, [
    { text: "Failure demo, measured twice", size: 15, bold: true, color: C.white, after: 4 },
    { text: "NO DUPLICATES", size: 26, bold: true, color: "8EE0A6", after: 4 },
    { text: "Alert \"Silver job silent\" fired 2 min 12 s and 2 min 54 s after the kill. The promise is within 3 minutes.", size: 13, color: "DCE6F2", after: 3 },
    { text: "The check covers Silver only.", size: 12, color: C.paleBlue, italic: true }]));
  slides.push({ bg: C.white, items, minutes: "1:15",
    notes: "[1:15] Reliability is built in layers. If the Spark job is killed, checkpoints hold the Kafka offsets and the restart resumes from them. If a message is delivered twice, the MERGE on the key inserts nothing. Late events are flagged and left out of live metrics, and the batch corrects them. Bad rows are quarantined. If Databricks is unavailable, there is a second Gold store backend that loads the same tables into Postgres. Airflow runs one DAG, the daily ad report at 03:00 UTC, with retries on API errors; a rerun returns the stored report. The box on the right is the proof: we killed the Silver job while data was flowing, watched Grafana's alert fire 2 minutes 12 and 2 minutes 54 seconds later in two runs, restarted it, and the duplicate check said no duplicates both times. To be exact, that check covers Silver only." });
}

// ============================== 10. Grafana pipeline health ===========================================================
slides.push({ bg: C.white, minutes: "1:15",
  items: shotSlide(10, "3  OBSERVABILITY AND BUSINESS INTELLIGENCE", "Grafana: is the pipeline alive and clean?", "pipeline_live",
    "Grafana, Pipeline live, captured at the end of the 13 October 2026 rehearsal. The generator had just stopped, so events per minute is 0.",
    "What the engineer reads", [
      [b("Traffic. "), r("Events, active sessions and orders per minute show whether the stream is alive.")],
      [b("Quality. "), r("Late-event rate 2.5% and quarantine share 1.4% sit under the alert limits of 5% and 2%.")],
      [b("Liveness. "), r("The Silver job reported 7 seconds ago. No report for 2 minutes fires an alert.")],
      [b("Batch lane. "), r("Gold update and publish age show 1 day in this capture, because the batch runs on demand.")],
      [b("Alert table. "), r("Each rule says when it fires and what to do.")],
    ], { imgBox: [0.6, 1.5, 6.5, 5.1] }),
  notes: "[1:15] Now the operational side. This is the Grafana dashboard Pipeline live, captured at the end of our 13 October rehearsal, which is why events per minute has dropped to zero: the generator had just stopped. Along the top: events per minute, active sessions, orders and product views with add to cart. In the middle are the two data quality gauges: late-event rate 2.5% and quarantine share 1.4%, both below the alert limits of 5% and 2%. Bottom right: the Silver job last reported 7 seconds ago, and if that number passes two minutes an alert fires. The orange tiles show the age of the last Gold update and publish, one day here, because the batch lane runs on demand. And at the bottom, every alert rule is written out with what to do when it fires. Six dashboards and eight alert rules are provisioned as code." });

// ============================== 11. Grafana operations ================================================================
slides.push({ bg: C.white, minutes: "1:00",
  items: shotSlide(11, "3  OBSERVABILITY AND BUSINESS INTELLIGENCE", "Grafana: is the 20-minute express promise holding?", "express_sla",
    "Grafana, Express SLA, rehearsal of 13 October 2026 with the promise shortened to 2 minutes.",
    "What operations reads", [
      [b("On-time rate 94.0% "), r("against the 95% target, so the tile is orange.")],
      [b("6 breaches in the last hour, "), r("0 orders at risk now, 2 open express orders.")],
      [b("By store and by order. "), r("The table names the store, the open orders closest to their promise and the minutes left.")],
      [b("Rehearsal setting. "), r("The promise was 2 minutes and some express orders were forced late, so that a breach and its alert can be shown live.")],
    ], { imgBox: [0.6, 1.5, 6.9, 5.1] }),
  notes: "[1:00] The second Grafana view is for operations. Express orders carry their own promise, 20 minutes in production. This dashboard shows the on-time rate for the last hour, here 94.0% against a 95% target, so the tile is orange; the number of breaches; orders at risk now, meaning at least 75% of the promise has passed; and a table of the open orders closest to their promise, with the minutes left. For the demo we shorten the promise to two minutes and force some express orders late, so that a breach and its alert appear live. The same family of dashboards shows live stock per store, out-of-stock products, and demand that arrived at an empty shelf." });

// ============================== 12. Power BI campaigns ================================================================
slides.push({ bg: C.white, minutes: "1:15",
  items: shotSlide(12, "3  OBSERVABILITY AND BUSINESS INTELLIGENCE", "Power BI: where should the next budget go?", "pbi_campaigns",
    "Power BI, Campaigns page. Simulated data. Ad numbers use the ad API's UTC day.",
    "What marketing reads", [
      [b("Headline ROAS is on delivered orders: 8.39. "), r("On placed orders it is higher everywhere (eid_sale_1: 21.55 placed, 11.67 delivered), because cash-on-delivery orders cancel or fail.")],
      [b("Cost per delivered order: "), r("377 for ramadan_offer_1, 184 for retarget_cart_1, 119 for eid_sale_1.")],
      [b("A possible decision: "), r("move budget toward eid_sale_1 and question ramadan_offer_1. Attribution is last touch, so it is a lead, not proof.")],
    ], { imgBox: [0.6, 1.5, 8.2, 5.1] }),
  notes: "[1:15] Power BI is the business layer, and it reads the Gold tables and the daily marts. This is the Campaigns page, on simulated data. The headline return on ad spend is measured on delivered orders: 8.39. Look at the chart on the left: for every campaign, return on placed orders is much higher than on delivered ones, for example 21.55 against 11.67 for the Eid sale. That gap is exactly the cancelled and failed cash-on-delivery orders, and it is the first question we set out to answer. On the right, cost per delivered order is 377 for the Ramadan offer and 119 for the Eid sale. A marketer could move budget toward the Eid sale and question the Ramadan offer. We state the limit: attribution is last touch, which favours bottom-funnel channels, and neither model proves incrementality." });

// ============================== 13. Power BI marketing and stock ======================================================
slides.push({ bg: C.white, minutes: "1:15",
  items: shotSlide(13, "3  OBSERVABILITY AND BUSINESS INTELLIGENCE", "Power BI: the bridge, ad spend sent to empty shelves", "pbi_marketing",
    "Power BI, Marketing and stock page, day minus one. Simulated data. An estimate, not a measured loss.",
    "What the bridge shows", [
      [b("Exposed spend 378, "), r("10.1% of spend: estimated spend that landed on products out of stock.")],
      [b("10.2% of landing clicks "), r("went to out-of-stock products. Revenue at risk: 71,653.")],
      [b("All three at-risk products "), r("are in one store: Nasr City dark store (Hijab set 33, Towel set 40, Abaya 53).")],
      [b("Label it honestly: "), r("a shopper may buy a substitute, so this is exposure, not loss.")],
    ], { imgBox: [0.6, 1.5, 8.2, 5.1] }),
  notes: "[1:15] This is the page that only a joined platform can build. Marketing and stock combines the ad spend of day minus one with the out-of-stock demand that operations saw in Grafana. Exposed spend is 378, which is 10.1% of that day's spend; 10.2% of landing clicks went to products that were out of stock; and revenue at risk, meaning add-to-carts on empty shelves times price, is 71,653. The table shows that all three at-risk products are in one store: Nasr City. A marketing manager would pause or redirect ads for those products until they are restocked. We label it as an estimate: the ad API only reports spend per campaign per day, so we assume spend is spread evenly over served landing clicks, and a shopper might buy a substitute." });

// ============================== 14. End-to-end story ==================================================================
{
  const items = chrome(14, "3  OBSERVABILITY AND BUSINESS INTELLIGENCE", "From a stock-out to next morning's report");
  const steps = [
    ["1", "Stock runs out", "A hero product at Nasr City reaches available = 0, about 4 minutes into the demo run."],
    ["2", "Demand continues", "Shoppers still view it and add it to the cart. Empty stock does not stop demand."],
    ["3", "Live sink flags it", "Each demand minute is marked: out of stock when the minute was written."],
    ["4", "Alert fires", "Grafana alert on demand for out-of-stock products fires 1 minute after the stock-out."],
    ["5", "Restock", "About 2 minutes after the stock-out. The out-of-stock count falls to 0."],
    ["6", "Next morning", "The D-1 mart prices the exposure. Power BI shows exposed spend and revenue at risk."],
  ];
  const cw = 1.95, gap = 0.08;
  steps.forEach((s, i) => {
    const x = 0.6 + i * (cw + gap), fill = i < 5 ? (i < 3 ? C.mist : "E3F1E8") : "E1ECFB";
    items.push(el.rect(x, 1.75, cw, 2.95, { fill, radius: 0.12, step: i + 1 }));
    items.push(el.rect(x + 0.15, 1.9, 0.46, 0.46, { fill: i < 5 ? C.green : C.navy, radius: 0.23, step: i + 1 }));
    items.push(el.text(x + 0.15, 1.9, 0.46, 0.46, s[0], { size: 16, bold: true, color: C.white, align: "center", valign: "middle", step: i + 1 }));
    items.push(el.text(x + 0.15, 2.45, cw - 0.3, 2.2, [{ text: s[1], size: 16, bold: true, color: C.navy, after: 3 }, { text: s[2], size: 13 }], { step: i + 1 }));
  });
  items.push(el.text(0.6, 4.9, 12.1, 0.3, "How the next morning's number is computed", { size: 14, bold: true, color: C.navy }));
  [["oos_click_share", "= out-of-stock landing clicks / served landing clicks"], ["exposed_spend", "= spend x oos_click_share"], ["revenue_at_risk", "= add-to-carts on out-of-stock products x price"]].forEach((f, i) => {
    const x = 0.6 + i * 4.085;
    items.push(el.rect(x, 5.3, 3.93, 0.95, { fill: C.code, radius: 0.1, step: 6 }));
    items.push(el.text(x + 0.15, 5.3, 3.63, 0.95, [{ text: f[0], size: 14, bold: true, color: "8EE0A6" }, { text: f[1], size: 12, color: "DCE6F2" }], { font: MONO, valign: "middle", step: 6 }));
  });
  items.push(el.text(0.6, 6.45, 12.1, 0.5, "Measured on 12 October 2026 on the demo: Galabeya 61 at the Nasr City dark store, stock-out at 21:16:14 Cairo time, alert 1 minute later, restock about 2 minutes later. Simulated data.", { size: 12, color: C.muted, italic: true }));
  slides.push({ bg: C.white, items, minutes: "1:15", steps: 6,
    notes: "[1:15] Let me tell the whole story once. In our demo scenario a popular product at the Nasr City dark store is deliberately made to run out. On 12 October the Galabeya 61 reached zero stock at 21:16:14, about four minutes into the run. Shoppers keep viewing it and adding it to the cart: empty stock does not stop demand, and that lost demand is what we measure. The live sink marks each demand minute as out of stock when the minute was written. One minute after the stock-out the Grafana alert fires, and about two minutes after, the product is restocked. That is the operations ending. The marketing ending comes the next morning: the daily mart takes the out-of-stock click share, multiplies it by the spend, and Power BI shows exposed spend and revenue at risk. One set of events, two users, one bridge." });
}

// ============================== 15. Quality and results ===============================================================
{
  const items = chrome(15, "4  QUALITY AND CLOSE", "Quality and measured results");
  const stats = [
    ["1,169", "host tests passed, 0 failed", "13 Oct 2026"], ["217", "Spark tests passed in Docker, 22 files", "12 Oct 2026"],
    ["66", "data quality checks, 0 failed, on the Gold build", "scratch catalog, 13 Oct"], ["654.73 s", "for 1,000,000 simulated events through the whole pipeline", "one laptop, 12 Oct"],
    ["2 of 2", "kill and restart runs ended with NO DUPLICATES", "12 Oct 2026"],
  ];
  const cw = 2.3, gap = 0.15;
  stats.forEach((s, i) => {
    const x = 0.6 + i * (cw + gap);
    items.push(el.rect(x, 1.8, cw, 2.95, { fill: i % 2 ? C.mist : "E1ECFB", radius: 0.12 }));
    items.push(el.text(x + 0.12, 1.95, cw - 0.24, 0.9, s[0], { size: i === 3 ? 30 : 36, bold: true, color: C.navy, align: "center", valign: "middle" }));
    items.push(el.text(x + 0.15, 2.95, cw - 0.3, 1.2, s[1], { size: 14, align: "center" }));
    items.push(el.text(x + 0.15, 4.2, cw - 0.3, 0.4, s[2], { size: 11, color: C.muted, align: "center", italic: true }));
  });
  items.push(...card(0.6, 5.0, 5.95, 1.8, "How to read these numbers", "The benchmark measures one setup (Docker limited to about 8 GB, a Databricks Free Edition SQL warehouse). It is not a capacity claim.", { fill: "FDF3D6", bodySize: 14, titleSize: 16 }));
  items.push(...card(6.75, 5.0, 5.95, 1.8, "Still open on 13 October", "One uninterrupted green run of the full smoke test with the Spark stage. The stages passed in two parts, and the three failures of the first run were fixed in the test scripts.", { fill: C.mist, bodySize: 14, titleSize: 16 }));
  slides.push({ bg: C.white, items, minutes: "1:15",
    notes: "[1:15] Quality is part of the product, so here are the measured results, each with its date. The full host test suite: 1,169 tests passed, none failed. The Spark tests, run in Docker one file at a time: 217 passed. The Gold build ran 66 data quality checks with no failure on the scratch catalog. The benchmark pushed 1,000,000 simulated events through the whole pipeline in 654.73 seconds on one laptop. And the failure demo ended with no duplicates in two runs out of two. Two honesty points. The benchmark is one setup and not a capacity claim. And one item is still open: a single uninterrupted green run of the whole smoke test including the Spark stage; its first full run had three failures that turned out to be test script issues and were fixed." });
}

// ============================== 16. Close =============================================================================
{
  const items = chrome(16, "4  QUALITY AND CLOSE", "What FunnelStream shows, and what comes next", { dark: true });
  const col = (x, title, lines, fill) => {
    items.push(el.rect(x, 1.75, 3.9, 4.1, { fill, radius: 0.12 }));
    items.push(el.text(x + 0.25, 1.95, 3.4, 3.8, [{ text: title, size: 19, bold: true, color: C.white, after: 8 }].concat(lines.map((l) => ({ text: l, size: 15, color: "DCE6F2", bullet: true, after: 7 })))));
  };
  col(0.6, "Shown", ["A streaming pipeline that survives a crash without duplicates", "Bad data quarantined with a reason, counted against the injected noise", "Two audiences served from the same events", "The bridge: exposed ad spend"], "123F6E");
  col(4.7, "Honest limits", ["All data is simulated", "Exposed spend is an estimate", "Stock is not put back on cancel or return", "Measured on one setup: one laptop, Databricks Free Edition"], "123F6E");
  col(8.8, "Next", ["Replay-safe stock intervals from Silver", "Stock reversal on cancel and return", "Great Expectations or dbt on the SQL checks", "Delivery zones and a Store dimension in Gold"], "123F6E");
  items.push(el.text(0.6, 6.1, 12.1, 0.8, "Thank you. Questions?", { size: 32, bold: true, color: C.white, align: "center", valign: "middle" }));
  slides.push({ bg: C.deep, dark: true, items, minutes: "1:30",
    notes: "[1:30] To close. FunnelStream shows a streaming pipeline that survives a crash without duplicates, bad data that is quarantined with a reason and counted against the noise we injected, and two audiences served from the same events, with one bridge between them: advertising spend sent to products that were out of stock. The honest limits: all data is simulated, exposed spend is an estimate, stock is not put back when an order is cancelled or returned, and the measurements come from one setup. What comes next: replay-safe stock intervals built from Silver, stock reversal on cancellation, Great Expectations or dbt on top of the SQL checks, and delivery zones with a Store dimension in Gold. Thank you. We are happy to take questions; the next four slides are backup." });
}

// ============================== Backup slides (not in the 20 minutes) =================================================
slides.push({ bg: C.white, backup: true, minutes: "backup",
  items: shotSlide(17, "BACKUP", "Power BI: Executive page", "pbi_executive",
    "Power BI, Executive page. Simulated data.", "What it shows", [
      [b("Revenue placed 2,410,979, delivered 1,914,103: "), r("79.4% delivered share, 5.8% cancel rate.")],
      [b("One-day peak on 9 October "), r("holds about 55% of all revenue in the simulated data. The page says so.")],
      [b("Where orders leak: "), r("failed-attempt rate is highest for vodafone_cash (27.6%) and fawry (25.1%).")],
    ], { imgBox: [0.6, 1.5, 8.2, 5.1] }),
  notes: "Backup. Executive page of the Power BI report." });
slides.push({ bg: C.white, backup: true, minutes: "backup",
  items: shotSlide(18, "BACKUP", "Power BI: Funnel page", "pbi_funnel",
    "Power BI, Funnel page. Simulated data.", "What it shows", [
      [b("16,617 sessions end in 1,723 orders: "), r("10.4% session conversion.")],
      [b("Cart abandon rate 69.1%, "), r("checkout abandon rate 30.7%.")],
      [b("By channel: "), r("direct has the highest cart abandon rate (70.9%); paid_social brings the most sessions (4.5K).")],
    ], { imgBox: [0.6, 1.5, 8.2, 5.1] }),
  notes: "Backup. Funnel page of the Power BI report." });
slides.push({ bg: C.white, backup: true, minutes: "backup",
  items: shotSlide(19, "BACKUP", "Grafana: live stock monitor", "live_stock_monitor",
    "Grafana, Live stock monitor, rehearsal of 13 October 2026. All stock numbers are simulated.", "What operations reads", [
      [b("537,542 units on hand, "), r("0 out-of-stock products, 1 low-stock product, in-stock rate 100%.")],
      [b("Restock watchlist: "), r("Bed sheets 20 at Nasr City has 1 unit against a reorder point of 3: restock now.")],
      [b("Demand on out-of-stock products "), r("per minute is the live version of the bridge.")],
    ], { imgBox: [0.6, 1.5, 6.9, 5.1] }),
  notes: "Backup. Live stock monitor dashboard." });
slides.push({ bg: C.white, backup: true, minutes: "backup",
  items: (() => {
    const it = chrome(20, "BACKUP", "Failure demo: kill, alert, restart, no duplicates");
    it.push(el.img("failure", 0.6, 1.5, 8.2, 5.2));
    it.push(el.text(9.15, 1.5, 3.55, 5.2, [{ text: "Measured twice, 12 October 2026", size: 18, bold: true, color: C.navy, after: 8 },
      { runs: [b("Alert fired "), r("2 min 12 s and 2 min 54 s after the kill.")], size: 15, bullet: true, after: 7 },
      { runs: [b("Heartbeat back "), r("under 30 s about 40 s and 35 s after the restart.")], size: 15, bullet: true, after: 7 },
      { runs: [b("NO DUPLICATES: "), r("8,469 events and 16,404 events in Silver.")], size: 15, bullet: true, after: 7 },
      { runs: [b("Scope: "), r("Silver only. It does not prove that no event was lost.")], size: 15, bullet: true }]));
    return it;
  })(),
  notes: "Backup. Sequence of the failure demo." });

module.exports = { slides, C, FONT, ROOT };
