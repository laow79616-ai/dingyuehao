_last_id = {}
import json, tempfile, shutil, requests
from pathlib import Path
import app as panel
import logging
logging.getLogger("werkzeug").setLevel(logging.ERROR)

from flask import request, jsonify

def _proxies():
    xs = panel.load_json(panel.DATA / "ip_pool.json", [])
    if not xs:
        return None
    x = xs[0]
    p = f"socks5h://{x['username']}:{x['password']}@{x['host']}:{x['port']}"
    return {"http": p, "https": p}

async def send_loop(gid):
    g = next((x for x in panel.load_json(panel.GROUP_FILE, []) if x.get("id") == gid), None)
    if not g:
        return {"status": "error", "message": "组不存在"}
    wid = g.get("worker_id")
    if wid not in panel._clients:
        await panel._start_worker(wid)
    client = panel._clients[wid]
    src = g.get("source_peer_id") or g.get("source")
    batch = list(await client.get_messages(src, limit=12))
    if not batch:
        return {"status": "error", "message": "源没有消息"}
    newest = int(getattr(batch[0], "id", 0) or 0)
    last = int(_last_id.get(gid) or 0)
    if last <= 0:
        _last_id[gid] = newest
        return {"status": "ok", "message": "记住最新id="+str(newest)}
    if newest <= last:
        return {"status": "ok", "message": "无新帖"}
    _last_id[gid] = newest
    latest = batch[0]
    gg = getattr(latest, "grouped_id", None)
    msgs = [m for m in batch if gg and getattr(m, "grouped_id", None) == gg] if gg else [latest]
    caption = ""
    for m in msgs:
        t = getattr(m, "message", None) or ""
        if t:
            caption = t
            break
    bots = panel.load_json(panel.DATA / "bots.json", [])
    px = _proxies()
    tmp = tempfile.mkdtemp()
    files = []
    try:
        for i, m in enumerate(msgs):
            if not getattr(m, "media", None):
                continue
            path = await client.download_media(m, file=str(Path(tmp) / ("m%d" % i)))
            if path:
                kind = "photo" if getattr(m, "photo", None) and not getattr(m, "video", None) else "video"
                files.append((path, kind))
        sent = []
        for tg in g.get("targets") or []:
            chat = tg.get("username")
            bot = next((b for b in bots if b.get("id") == tg.get("bot_id")), None)
            if not bot:
                sent.append("%s:未指定Bot" % chat)
                continue
            token = bot["token"]
            try:
                if not files:
                    r = requests.post("https://api.telegram.org/bot%s/sendMessage" % token,
                                      json={"chat_id": chat, "text": caption or " "},
                                      timeout=60, proxies=px)
                elif len(files) == 1:
                    path, kind = files[0]
                    url = "https://api.telegram.org/bot%s/%s" % (token, "sendPhoto" if kind == "photo" else "sendVideo")
                    field = "photo" if kind == "photo" else "video"
                    with open(path, "rb") as fh:
                        r = requests.post(url, data={"chat_id": chat, "caption": (caption or "")[:1024]},
                                          files={field: fh}, timeout=180, proxies=px)
                else:
                    media, fs = [], {}
                    for i, (path, kind) in enumerate(files):
                        key = "f%d" % i
                        item = {"type": "photo" if kind == "photo" else "video", "media": "attach://" + key}
                        if i == 0 and caption:
                            item["caption"] = caption[:1024]
                        media.append(item)
                        fs[key] = open(path, "rb")
                    r = requests.post("https://api.telegram.org/bot%s/sendMediaGroup" % token,
                                      data={"chat_id": chat, "media": json.dumps(media)},
                                      files=fs, timeout=180, proxies=px)
                    for fh in fs.values():
                        fh.close()
                data = r.json()
                if data.get("ok"):
                    sent.append("%s:%s:ok" % (chat, bot.get("remark")))
                else:
                    sent.append("%s:%s" % (chat, data.get("description")))
            except Exception as e:
                sent.append("%s:%s" % (chat, e))
        return {"status": "ok", "message": " ; ".join(sent)}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

def send_loop_api(gid):
    try:
        return jsonify(panel.run_async(send_loop(gid), timeout=180))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

panel.app.add_url_rule("/api/groups/<gid>/collect-now", "send_loop_api", send_loop_api, methods=["POST"])

def bots_list_v3():
    from flask import jsonify
    return jsonify({"ok": True, "bots": panel.load_json(panel.DATA/"bots.json", [])})
panel.app.add_url_rule("/api/botlist","bots_list_v3",bots_list_v3,methods=["GET"])
print("send-loop ready")



import threading, time
_last_id = {}

def _auto_loop():
    time.sleep(10)
    while True:
        try:
            gs = panel.load_json(panel.GROUP_FILE, [])
            for g in gs:
                try:
                    r = panel.run_async(send_loop(g["id"]), timeout=180)
                    print("auto", r)
                except Exception as e:
                    print("auto err", e)
        except Exception as e:
            print("auto loop", e)
        time.sleep(15)

if __name__ == "__main__":
    threading.Thread(target=_auto_loop, daemon=True).start()

    panel.start_loop()
    panel.app.run(host="0.0.0.0", port=8088, debug=False, use_reloader=False)
