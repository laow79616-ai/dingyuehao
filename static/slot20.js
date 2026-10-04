window.__slot20 = window.__slot20 || 1;
window.paintSlot20 = async function () {
  var d = await fetch("/api/overview").then(function (x) { return x.json(); });
  var g = (d.groups || [])[0] || {};
  var targets = g.targets || [];
  var bots = d.bots || [];
  var workers = d.workers || [];
  var w = workers.find(function (x) { return x.id === g.worker_id; }) || workers[0] || {};
  var box = document.getElementById("worker-cols");
  if (!box) return;
  var left = "";
  for (var n = 1; n <= 20; n++) {
    var cnt = targets.filter(function (x) { return Number(x.slot || 0) === n; }).length;
    var on = n === window.__slot20;
    left += '<div data-s20="' + n + '" style="padding:8px 10px;cursor:pointer;border-bottom:1px solid #163;background:' + (on ? "#123" : "transparent") + ';">' + (on ? "● " : "") + "第" + n + "组<div class=\"muted\">" + cnt + "/10</div></div>";
  }
  var rows = targets.filter(function (x) { return Number(x.slot || 0) === window.__slot20; });
  var body = rows.map(function (tg) {
    var name = tg.username || "";
    var opts = '<option value="">选择中文Bot</option>' + bots.map(function (b) {
      return '<option value="' + b.id + '"' + (tg.bot_id === b.id ? " selected" : "") + ">" + (b.remark || "Bot") + "</option>";
    }).join("");
    return '<div style="display:flex;gap:8px;align-items:center;border-bottom:1px solid #163;padding:8px 12px;"><b style="flex:1">' + name + '</b><select style="background:#111;color:#fff;border:1px solid #1f8f4a;border-radius:6px;padding:6px 8px;min-width:240px;">' + opts + '</select></div>';
  }).join("") || '<div class="muted" style="padding:12px;">本组还没有目标</div>';
  box.innerHTML = '<div style="display:grid;grid-template-columns:180px 1fr;width:100%;align-items:start;border:1px solid #22c55e;margin-top:0;"><div style="border-right:1px solid #22c55e;max-height:640px;overflow:auto;align-self:start;">' + left + '</div><div><div style="padding:8px 12px;border-bottom:1px solid #22c55e;">' + (g.source || "") + " · 第" + window.__slot20 + "组 · 水军 " + (w.phone || "") + " · " + rows.length + "/10</div>" + body + "</div></div>";
  box.querySelectorAll("[data-s20]").forEach(function (el) {
    el.onclick = function () { window.__slot20 = Number(el.dataset.s20); window.paintSlot20(); };
  });
};
paintSlot20();
setInterval(paintSlot20, 8000);
