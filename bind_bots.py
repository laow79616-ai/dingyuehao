import json, tempfile, shutil
from pathlib import Path
import app as panel
from flask import request, jsonify
BOT_PATH = panel.DATA / "bots.json"
_last_id = {}


def bots_add_v5():
    body = request.get_json(silent=True) or {}
    token = (body.get("token") or "").strip()
    remark = (body.get("remark") or "").strip()
    if not token or ":" not in token:
        return jsonify({"status":"error","message":"请填写Token"})
    with panel._lock:
        bots = panel.load_json(BOT_PATH, [])
        if any(b.get("token")==token for b in bots):
            return jsonify({"status":"ok","message":"已存在","bots":bots})
        bots.append({"id": panel.new_id("bot_"), "token": token, "remark": remark or "bot", "created_at": panel.now_str()})
        panel.save_json(BOT_PATH, bots)
    return jsonify({"status":"ok","bots":bots})

def bots_list_v3():
    try:
        bots = panel.load_json(BOT_PATH, [])
    except Exception:
        bots = []
    return jsonify({"ok": True, "bots": bots})

async def _collect_publish(gid):
    import requests
    g = next((x for x in panel.load_json(panel.GROUP_FILE, []) if x.get("id")==gid), None)
    if not g: return {"status":"error","message":"组不存在"}
    try:
        await panel._restore_sessions()
    except Exception:
        pass
    wid = g.get("worker_id")
    if wid and wid not in panel._clients:
        try:
            await panel._start_worker(wid)
        except Exception:
            pass
    if not wid or wid not in panel._clients:
        online = [w["id"] for w in panel.load_json(panel.WORKER_FILE, []) if w["id"] in panel._clients]
        if not online:
            return {"status":"error","message":"没有在线水军，无法读源"}
        wid = online[0]
    client = panel._clients[wid]
    src = g.get("source_peer_id") or g.get("source")
    try:
        batch = list(await client.get_messages(src, limit=15))
    except Exception as e:
        remark = (g.get("source_remark") or "").strip()
        batch = []
        if remark:
            async for d in client.iter_dialogs(limit=80):
                title = getattr(d,"name",None) or ""
                if remark in title:
                    batch = list(await client.get_messages(d.entity, limit=15))
                    break
        if not batch:
            return {"status":"error","message":"读取源失败: "+str(e)}
    if not batch:
        return {"status":"error","message":"源没有消息"}
    newest = int(getattr(batch[0], "id", 0) or 0)
    gid = g.get("id")
    last = int(_last_id.get(gid) or 0)
    if last <= 0:
        _last_id[gid] = newest
        return {"status":"ok","message":"记住最新id="+str(newest)}
    if newest <= last:
        return {"status":"ok","message":"无新帖"}
    _last_id[gid] = newest
    latest = batch[0]
    gg = getattr(latest,"grouped_id",None)
    msgs = [m for m in batch if gg and getattr(m,"grouped_id",None)==gg] if gg else [latest]
    chat = (g.get("targets") or [{}])[0].get("username")
    try:
        caption = ""
        for m in msgs:
            t = (getattr(m,"message",None) or "") or ""
            if t:
                caption = t
                break
        files = [m.media for m in msgs if getattr(m,"media",None)]
        if files:
            await client.send_file(chat, files, caption=caption[:1024] if caption else None)
        elif caption:
            await client.send_message(chat, caption)
        return {"status":"ok","message":"采集发布: %s:copy:%d" % (chat, len(msgs))}
    except Exception as e:
        return {"status":"error","message":"发布失败: "+str(e)}

def collect_now_v3(gid):
    try:
        return jsonify(panel.run_async(_collect_publish(gid), timeout=180))
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})

panel.app.add_url_rule("/api/bots","bots_add_v5",bots_add_v5,methods=["POST"])
panel.app.add_url_rule("/api/botlist","bots_list_v3",bots_list_v3,methods=["GET"])
panel.app.add_url_rule("/api/groups/<gid>/collect-now","collect_now_v4",collect_now_v3,methods=["POST"])
print("collect-now v4 ready")



def _auto_loop():
    import time
    time.sleep(8)
    while True:
        try:
            for g in panel.load_json(panel.GROUP_FILE, []):
                try:
                    panel.run_async(_collect_publish(g["id"]), timeout=120)
                except Exception as e:
                    print("auto", e)
        except Exception as e:
            print("auto loop", e)
        time.sleep(8)

def group_bot_v5(gid):
    body = request.get_json(silent=True) or {}
    ids = body.get("bot_ids") or []
    if isinstance(ids, str):
        ids = [ids]
    with panel._lock:
        gs = panel.load_json(panel.GROUP_FILE, [])
        for g in gs:
            if g.get("id")==gid:
                g["bot_id"] = body.get("bot_id") or (ids[0] if ids else "")
                g["bot_ids"] = [x for x in ids if x]
        panel.save_json(panel.GROUP_FILE, gs)
    return jsonify({"status":"ok","message":"已保存"+str(len(ids))+"个Bot"})

def group_bot_v5(gid):
    body = request.get_json(silent=True) or {}
    ids = body.get("bot_ids") or []
    if isinstance(ids, str):
        ids = [ids]
    with panel._lock:
        gs = panel.load_json(panel.GROUP_FILE, [])
        for g in gs:
            if g.get("id")==gid:
                g["bot_id"] = body.get("bot_id") or (ids[0] if ids else "")
                g["bot_ids"] = [x for x in ids if x]
        panel.save_json(panel.GROUP_FILE, gs)
    return jsonify({"status":"ok","message":"已保存Bot"})

panel.app.add_url_rule("/api/groups/<gid>/bot","group_bot_v5",group_bot_v5,methods=["POST"])

async def _collect_all_targets(gid):
    g = next((x for x in panel.load_json(panel.GROUP_FILE, []) if x.get("id")==gid), None)
    if not g:
        return {"status":"error","message":"组不存在"}
    try:
        await panel._restore_sessions()
    except Exception:
        pass
    wid = g.get("worker_id")
    if wid and wid not in panel._clients:
        try:
            await panel._start_worker(wid)
        except Exception:
            pass
    if not wid or wid not in panel._clients:
        online = [w["id"] for w in panel.load_json(panel.WORKER_FILE, []) if w["id"] in panel._clients]
        if not online:
            return {"status":"error","message":"没有在线水军"}
        wid = online[0]
    client = panel._clients[wid]
    src = g.get("source_peer_id") or g.get("source")
    batch = list(await client.get_messages(src, limit=12))
    if not batch:
        return {"status":"error","message":"源没有消息"}
    latest = batch[0]
    gg = getattr(latest, "grouped_id", None)
    msgs = [m for m in batch if gg and getattr(m,"grouped_id",None)==gg] if gg else [latest]
    caption = ""
    for m in msgs:
        t = (getattr(m,"message",None) or "") or ""
        if t:
            caption = t
            break
    files = [m.media for m in msgs if getattr(m,"media",None)]
    sent = []
    for tg in (g.get("targets") or []):
        chat = tg.get("username") or tg.get("peer_id")
        try:
            if files:
                await client.send_file(chat, files, caption=caption[:1024] if caption else None)
            elif caption:
                await client.send_message(chat, caption)
            sent.append(str(chat)+":ok")
        except Exception as e:
            sent.append(str(chat)+":"+str(e))
    return {"status":"ok","message":" ; ".join(sent)}

def collect_now_v6(gid):
    try:
        return jsonify(panel.run_async(_collect_all_targets(gid), timeout=180))
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})

panel.app.add_url_rule("/api/groups/<gid>/collect-now","collect_now_v6",collect_now_v6,methods=["POST"])
print("all-targets ready")

def push_all_v7(gid):
    try:
        return jsonify(panel.run_async(_collect_all_targets(gid), timeout=180))
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})
panel.app.add_url_rule("/api/groups/<gid>/push-all","push_all_v7",push_all_v7,methods=["POST"])
print("push-all ready")

def botsave_v8(gid):
    body = request.get_json(silent=True) or {}
    ids = body.get("bot_ids") or []
    if isinstance(ids, str):
        ids = [ids]
    with panel._lock:
        gs = panel.load_json(panel.GROUP_FILE, [])
        for g in gs:
            if g.get("id")==gid:
                g["bot_id"] = body.get("bot_id") or (ids[0] if ids else "")
                g["bot_ids"] = [x for x in ids if x]
        panel.save_json(panel.GROUP_FILE, gs)
    return jsonify({"status":"ok","message":"已保存%d个Bot"%len(ids)})
panel.app.add_url_rule("/api/groups/<gid>/botsave","botsave_v8",botsave_v8,methods=["POST"])
print("botsave ready")

def _pick_bot(target, bots):
    text = ((target.get("username") or "") + " " + (target.get("remark") or "")).lower()
    for b in bots:
        r = (b.get("remark") or "").lower()
        if "vam" in r or "国漫" in r:
            if "vam" in text or "anime" in text or "国漫" in text:
                return b
        if "快约" in r and "kuaiyue" in text:
            return b
    return bots[0] if bots else None
print("bot-map ready")

def target_bot_v9(gid):
    body = request.get_json(silent=True) or {}
    tid = body.get("target_id") or ""
    bot_id = body.get("bot_id") or ""
    with panel._lock:
        gs = panel.load_json(panel.GROUP_FILE, [])
        for g in gs:
            if g.get("id")!=gid: continue
            for t in g.get("targets") or []:
                if t.get("id")==tid:
                    t["bot_id"]=bot_id
        panel.save_json(panel.GROUP_FILE, gs)
    return jsonify({"status":"ok","message":"该目标已指定Bot"})
panel.app.add_url_rule("/api/groups/<gid>/target-bot","target_bot_v9",target_bot_v9,methods=["POST"])
print("target-bot ready")

if __name__ == '__main__':
    import threading
    panel.start_loop()
    threading.Thread(target=_auto_loop, daemon=True).start()
    panel.app.run(host='0.0.0.0', port=8088, debug=False, use_reloader=False)



