import json, zipfile, shutil
from pathlib import Path
from flask import request, jsonify, send_from_directory
import app as panel
ROOT = Path(__file__).resolve().parent
DATA = panel.DATA
GROUP_FILE = DATA / "worker_groups.json"
MAX_N = 10
def load_groups():
    return panel.load_json(GROUP_FILE, [])
def save_groups(rows):
    panel.save_json(GROUP_FILE, rows)
def api_hub():
    return jsonify({"status":"ok","groups":load_groups(),"workers":panel.load_json(panel.WORKER_FILE, []),"bots":panel.load_json(DATA/"bots.json", []),"max":MAX_N})
def api_group_add():
    data = request.get_json(silent=True) or {}
    rows = load_groups()
    g = {"id": panel.new_id("wg_"), "name": (data.get("name") or "水军组").strip(), "worker_ids": [], "target_usernames": [], "bot_ids": [], "created_at": panel.now_str()}
    rows.append(g)
    save_groups(rows)
    return jsonify({"status":"ok","group":g})
def api_group_bind_worker(gid):
    data = request.get_json(silent=True) or {}
    wid = (data.get("worker_id") or "").strip()
    rows = load_groups()
    for g in rows:
        if g.get("id") != gid:
            continue
        ids = g.setdefault("worker_ids", [])
        if wid in ids:
            return jsonify({"status":"ok"})
        if len(ids) >= MAX_N:
            return jsonify({"status":"error","message":"一组最多10个水军"})
        ids.append(wid)
        save_groups(rows)
        return jsonify({"status":"ok","group":g})
    return jsonify({"status":"error","message":"组不存在"})
def api_group_bind_target(gid):
    data = request.get_json(silent=True) or {}
    name = (data.get("username") or "").strip()
    if name and not name.startswith("@"):
        name = "@" + name
    rows = load_groups()
    for g in rows:
        if g.get("id") != gid:
            continue
        targets = g.setdefault("target_usernames", [])
        if len(targets) >= MAX_N:
            return jsonify({"status":"error","message":"一组最多10个目标"})
        if name not in targets:
            targets.append(name)
        save_groups(rows)
        return jsonify({"status":"ok","group":g})
    return jsonify({"status":"error","message":"组不存在"})
def api_group_bind_bot(gid):
    data = request.get_json(silent=True) or {}
    bid = (data.get("bot_id") or "").strip()
    rows = load_groups()
    for g in rows:
        if g.get("id") != gid:
            continue
        bots = g.setdefault("bot_ids", [])
        if bid and bid not in bots:
            bots.append(bid)
        save_groups(rows)
        return jsonify({"status":"ok","group":g})
    return jsonify({"status":"error","message":"组不存在"})
def api_import_zip():
    f = request.files.get("file")
    if not f:
        return jsonify({"status":"error","message":"没有文件"})
    tmp = Path("/tmp/hub-import")
    if tmp.exists():
        shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)
    zpath = tmp / "in.zip"
    f.save(zpath)
    n = 0
    workers = panel.load_json(panel.WORKER_FILE, [])
    with zipfile.ZipFile(zpath) as zf:
        zf.extractall(tmp / "out")
    sess = ROOT / "sessions"
    sess.mkdir(exist_ok=True)
    for p in (tmp / "out").rglob("*.session"):
        wid = panel.new_id("w_")
        shutil.copy(p, sess / (wid + ".session"))
        workers.append({"id": wid, "phone": p.stem, "remark": p.stem, "status": "imported", "created_at": panel.now_str()})
        n += 1
    panel.save_json(panel.WORKER_FILE, workers)
    return jsonify({"status":"ok","imported":n})
def api_match_api():
    workers = panel.load_json(panel.WORKER_FILE, [])
    apis = panel.load_json(DATA / "api_pool.json", [])
    used = {}
    for w in workers:
        if w.get("api_id"):
            used[w["api_id"]] = used.get(w["api_id"], 0) + 1
    changed = 0
    for w in workers:
        if w.get("api_id"):
            continue
        for a in apis:
            aid = a.get("id")
            if used.get(aid, 0) < MAX_N:
                w["api_id"] = aid
                used[aid] = used.get(aid, 0) + 1
                changed += 1
                break
    panel.save_json(panel.WORKER_FILE, workers)
    return jsonify({"status":"ok","matched":changed})
def page_hub():
    return send_from_directory(ROOT / "static", "hub.html")
panel.app.before_request_funcs[None] = []
panel.app.add_url_rule("/hub", "page_hub", page_hub)
panel.app.add_url_rule("/api/hub", "api_hub", api_hub)
panel.app.add_url_rule("/api/hub/groups", "api_group_add", api_group_add, methods=["POST"])
panel.app.add_url_rule("/api/hub/groups/<gid>/worker", "api_group_bind_worker", api_group_bind_worker, methods=["POST"])
panel.app.add_url_rule("/api/hub/groups/<gid>/target", "api_group_bind_target", api_group_bind_target, methods=["POST"])
panel.app.add_url_rule("/api/hub/groups/<gid>/bot", "api_group_bind_bot", api_group_bind_bot, methods=["POST"])
panel.app.add_url_rule("/api/hub/import-zip", "api_import_zip", api_import_zip, methods=["POST"])
panel.app.add_url_rule("/api/hub/match-api", "api_match_api", api_match_api, methods=["POST"])
print("worker-hub ready")
if __name__ == "__main__":
    panel.start_loop()
    panel.app.run(host="0.0.0.0", port=8088, debug=False, use_reloader=False)
