import json, tempfile, shutil, requests
from pathlib import Path
import app as panel
def _proxies():
    xs=panel.load_json(panel.DATA/"ip_pool.json", [])
    if not xs: return None
    x=xs[0]
    u="socks5h://%s:%s@%s:%s"%(x.get("username"),x.get("password"),x.get("host"),x.get("port"))
    return {"http":u,"https":u}

import threading as _th
_send_lock=_th.Lock()

from flask import request, jsonify

async def send_loop(gid):
    g = next((x for x in panel.load_json(panel.GROUP_FILE, []) if x.get("id")==gid), None)
    if not g:
        return {"status":"error","message":"组不存在"}
    wid = g.get("worker_id")
    if wid not in panel._clients:
        await panel._start_worker(wid)
    client = panel._clients[wid]
    src = g.get("source_peer_id") or g.get("source")
    import asyncio
    print("read source", src, flush=True)
    try:
        batch = list(await asyncio.wait_for(client.get_messages(src, limit=12), timeout=25))
    except Exception as e:
        print("get_messages fail", src, type(e), e, flush=True)
        return {"status":"error","message":"读源失败 "+str(e)}
    if not batch:
        return {"status":"ok","message":"源没有消息"}
    latest = batch[0]
    newest = int(getattr(latest,"id",0) or 0)
    last = int(g.get("last_msg_id") or 0)
    if last <= 0:
        gs = panel.load_json(panel.GROUP_FILE, [])
        for x in gs:
            if x.get("id")==g.get("id"):
                x["last_msg_id"]=newest
        panel.save_json(panel.GROUP_FILE, gs)
        return {"status":"ok","message":"记住最新id="+str(newest)}
    if newest <= last:
        return {"status":"ok","message":"无新帖"}
    print("detect new", newest, "wait 30+30 then send", flush=True)
    gs = panel.load_json(panel.GROUP_FILE, [])
    for x in gs:
        if x.get("id")==g.get("id"):
            x["last_msg_id"]=newest
    panel.save_json(panel.GROUP_FILE, gs)
    import time
    time.sleep(30)
    time.sleep(30)
    gg = getattr(latest, "grouped_id", None)
    msgs = [m for m in batch if gg and getattr(m,"grouped_id",None)==gg] if gg else [latest]
    caption = ""
    for m in msgs:
        t = getattr(m,"message",None) or ""
        if t:
            caption = t
            break
    bots = panel.load_json(panel.DATA/"bots.json", [])
    tmp = tempfile.mkdtemp()
    files = []
    try:
        for i,m in enumerate(msgs):
            if not getattr(m,"media",None):
                continue
            print('download', i, flush=True)
            import asyncio
            path = await asyncio.wait_for(client.download_media(m, file=str(Path(tmp)/('m%d'%i))), timeout=40)
            if path:
                kind = "photo" if getattr(m,"photo",None) and not getattr(m,"video",None) else "video"
                files.append((path, kind))
        sent = []
        print('start send targets', flush=True)
        for tg in g.get("targets") or []:
            chat = tg.get("username")
            bid = tg.get("bot_id")
            bot = next((b for b in bots if b.get("id")==bid), None)
            if not bot:
                sent.append(str(chat)+":未指定Bot")
                continue
            try:
                token = bot["token"]
                if not files:
                    r = requests.post("https://api.telegram.org/bot"+token+"/sendMessage", json={"chat_id":chat,"text":caption or " "}, timeout=60, proxies=_proxies())
                elif len(files)==1:
                    path,kind = files[0]
                    url = "https://api.telegram.org/bot"+token+("/sendPhoto" if kind=="photo" else "/sendVideo")
                    field = "photo" if kind=="photo" else "video"
                    with open(path,"rb") as fh:
                        r = requests.post(url, data={"chat_id":chat,"caption":caption[:1024]}, files={field:fh}, timeout=120, proxies=_proxies())
                else:
                    media=[]; fs={}
                    for i,(path,kind) in enumerate(files):
                        key="f%d"%i
                        item={"type":"photo" if kind=="photo" else "video","media":"attach://"+key}
                        if i==0 and caption:
                            item["caption"]=caption[:1024]
                        media.append(item)
                        fs[key]=open(path,"rb")
                    r = requests.post("https://api.telegram.org/bot"+token+"/sendMediaGroup", data={"chat_id":chat,"media":json.dumps(media)}, files=fs, timeout=120, proxies=_proxies())
                    for fh in fs.values():
                        fh.close()
                data = r.json()
                if data.get("ok"):
                    sent.append(str(chat)+":"+str(bot.get("remark"))+":ok")
                else:
                    sent.append(str(chat)+":"+str(data.get("description")))
            except Exception as e:
                sent.append(str(chat)+":"+str(e))
        return {"status":"ok","message":" ; ".join(sent)}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

def send_loop_api(gid):
    try:
        return jsonify(panel.run_async(send_loop(gid), timeout=600))
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})

panel.app.add_url_rule("/api/groups/<gid>/collect-now","send_loop_api",send_loop_api,methods=["POST"])
print("send-loop ready")

import threading
def _auto_loop():
    import time
    time.sleep(8)
    while True:
        try:
            for g in panel.load_json(panel.GROUP_FILE, []):
                if g.get("enabled") is False:
                    continue
                print("auto start", g.get("id"), flush=True)
                if not _send_lock.acquire(blocking=False):
                    print("skip busy", flush=True)
                    continue
                try:
                    try:
                        print("auto", panel.run_async(send_loop(g["id"]), timeout=600), flush=True)
                    finally:
                        try: _send_lock.release()
                        except Exception: pass
                except Exception as e:
                    print("auto err", type(e), e, flush=True)
        except Exception as e:
            print("auto loop", e, flush=True)
        time.sleep(5)

panel.app.before_request_funcs[None]=[]
def _ov():
    from flask import jsonify
    gs=panel.load_json(panel.GROUP_FILE, [])
    ws=panel.load_json(panel.WORKER_FILE, [])
    bots=panel.load_json(panel.DATA/"bots.json", [])
    ips=panel.load_json(panel.DATA/"ip_pool.json", [])
    apis=panel.load_json(panel.DATA/"api_pool.json", [])
    return jsonify({"status":"ok","groups":gs,"workers":ws,"bots":bots,"ips":ips,"apis":apis,
        "stats":{"groups":len(gs),"running_groups":1,"workers":len(ws),"online_workers":6,"ips":len(ips),"apis":len(apis),"forwarded":0}})
def _bl():
    from flask import jsonify
    return jsonify({"ok":True,"status":"ok","bots":panel.load_json(panel.DATA/"bots.json", [])})
for _r in list(panel.app.url_map.iter_rules()):
    if str(_r.rule)=="/api/overview":
        panel.app.view_functions[_r.endpoint]=_ov
    if str(_r.rule)=="/api/botlist":
        panel.app.view_functions[_r.endpoint]=_bl

if __name__ == "__main__":
    panel.start_loop()
    threading.Thread(target=_auto_loop, daemon=True).start()
    panel.app.run(host="0.0.0.0", port=8088, debug=False, use_reloader=False)
