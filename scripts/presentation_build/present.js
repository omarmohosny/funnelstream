(function () {
  var stage = document.getElementById("stage");
  var slides = Array.prototype.slice.call(stage.querySelectorAll(".slide"));
  var notesPanel = document.getElementById("notesPanel");
  var help = document.getElementById("help");
  var clock = document.getElementById("clock"), count = document.getElementById("count"), planned = document.getElementById("planned");
  var cur = 0, step = 0, started = null, ticker = null;

  function fit() {
    var s = Math.min(window.innerWidth / 1333.3, window.innerHeight / 750);
    stage.style.transform = "scale(" + s + ")";
  }
  function maxStep(i) { return parseInt(slides[i].getAttribute("data-steps") || "0", 10); }
  function applySteps() {
    var els = slides[cur].querySelectorAll("[data-step]");
    for (var k = 0; k < els.length; k++) {
      var on = parseInt(els[k].getAttribute("data-step"), 10) <= step;
      if (els[k].classList) els[k].classList.toggle("on", on);
    }
  }
  function show(i, s) {
    cur = Math.max(0, Math.min(slides.length - 1, i));
    step = s == null ? 0 : s;
    slides.forEach(function (sl, k) { sl.classList.toggle("active", k === cur); });
    applySteps();
    var sl = slides[cur];
    count.textContent = (cur + 1) + " / " + slides.length + (sl.getAttribute("data-backup") === "1" ? "  backup" : "");
    planned.textContent = sl.getAttribute("data-min") === "backup" ? "" : "planned " + sl.getAttribute("data-min");
    notesPanel.innerHTML = sl.querySelector(".notes").innerHTML;
    if (history.replaceState) history.replaceState(null, "", "#" + (cur + 1));
  }
  function next() { if (step < maxStep(cur)) { step++; applySteps(); } else if (cur < slides.length - 1) show(cur + 1, 0); }
  function prev() { if (step > 0) { step--; applySteps(); } else if (cur > 0) show(cur - 1, maxStep(cur - 1)); }
  function tick() {
    var t = Math.floor((Date.now() - started) / 1000), m = Math.floor(t / 60), s = t % 60;
    clock.textContent = (m < 10 ? "0" : "") + m + ":" + (s < 10 ? "0" : "") + s;
    clock.classList.toggle("over", t > 1200);
  }
  function toggleTimer() {
    if (started) { clearInterval(ticker); started = null; clock.textContent = "00:00"; clock.classList.remove("over"); }
    else { started = Date.now(); ticker = setInterval(tick, 500); tick(); }
  }
  function overview() {
    var grid = document.getElementById("grid");
    if (!grid) {
      grid = document.createElement("div"); grid.id = "grid"; document.body.appendChild(grid);
      slides.forEach(function (sl, k) {
        var t = document.createElement("div"); t.className = "thumb";
        var inner = document.createElement("div"); inner.className = "inner";
        var c = sl.cloneNode(true); c.classList.remove("active"); inner.appendChild(c);
        t.appendChild(inner); var n = document.createElement("b"); n.textContent = k + 1; t.appendChild(n);
        t.addEventListener("click", function () { document.body.classList.remove("overview"); show(k, 0); });
        grid.appendChild(t);
      });
    }
    document.body.classList.toggle("overview");
    var w = grid.querySelector(".thumb").clientWidth;
    Array.prototype.forEach.call(grid.querySelectorAll(".inner"), function (i) { i.style.transform = "scale(" + (w / 1333.3) + ")"; });
  }
  document.addEventListener("keydown", function (e) {
    var k = e.key;
    if (k === "ArrowRight" || k === "PageDown" || k === " " || k === "Enter") { e.preventDefault(); next(); }
    else if (k === "ArrowLeft" || k === "PageUp" || k === "Backspace") { e.preventDefault(); prev(); }
    else if (k === "Home") show(0, 0);
    else if (k === "End") show(slides.length - 1, 0);
    else if (k === "o" || k === "O" || k === "Escape") overview();
    else if (k === "n" || k === "N") notesPanel.hidden = !notesPanel.hidden;
    else if (k === "t" || k === "T") toggleTimer();
    else if (k === "h" || k === "H") help.hidden = !help.hidden;
    else if (k === "f" || k === "F") { if (document.fullscreenElement) document.exitFullscreen(); else document.documentElement.requestFullscreen(); }
  });
  stage.addEventListener("click", function (e) { if (e.clientX > window.innerWidth / 3) next(); else prev(); });
  window.addEventListener("resize", function () { fit(); if (document.body.classList.contains("overview")) { overview(); overview(); } });
  fit();
  var q = new URLSearchParams(location.search);
  var start = parseInt(q.get("s") || (location.hash || "#1").slice(1), 10) || 1;
  show(start - 1, q.get("all") ? 99 : 0);
  if (q.get("all")) { var s = slides[cur].querySelectorAll("[data-step]"); for (var i = 0; i < s.length; i++) s[i].classList.add("on"); }
  if (!q.get("all") && !q.get("s") && !location.hash) help.hidden = false;
  if (q.get("notes")) notesPanel.hidden = false; else notesPanel.hidden = true;
})();
