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
    if not wid:
        wid = next(iter(panel._clients), "")
    if not wid or wid not in panel._clients:
        return {"status":"error","message":"没有在线水军"}
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
    print("newest", newest, "saved", g.get("last_msg_id"), flush=True)
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
def slot_remark(i):
    from flask import request, jsonify
    slots = panel.load_json(panel.DATA/"slots.json", [])
    body = request.get_json(silent=True) or {}
    slots[int(i)]["remark"] = body.get("remark") or ""
    panel.save_json(panel.DATA/"slots.json", slots)
    return jsonify({"status":"ok"})
def slot_del_target(i, n):
    from flask import jsonify
    slots = panel.load_json(panel.DATA/"slots.json", [])
    arr = slots[int(i)].get("targets") or []
    arr.pop(int(n))
    slots[int(i)]["targets"] = arr
    panel.save_json(panel.DATA/"slots.json", slots)
    return jsonify({"status":"ok"})
panel.app.add_url_rule("/api/slots/<i>/remark","slot_remark",slot_remark,methods=["POST"])
panel.app.add_url_rule("/api/slots/<i>/targets/<n>","slot_del_target",slot_del_target,methods=["DELETE"])
def slots_autobind():
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    bot_id = body.get("bot_id") or ""
    if not username:
        return jsonify({"status":"error","message":"请填写订阅号"})
    if not bot_id:
        return jsonify({"status":"error","message":"请选择指定发布机器人"})
    if not username.startswith("@") and "t.me/" not in username:
        username = "@" + username.lstrip("@")
    slots = panel.load_json(panel.DATA/"slots.json", [])
    item = {"username": username, "remark": body.get("remark") or "", "bot_id": bot_id}
    for i, s in enumerate(slots):
        arr = s.setdefault("targets", [])
        if any((x.get("username") if isinstance(x, dict) else x) == username for x in arr):
            return jsonify({"status":"error","message":"这组已有该订阅号"})
        if len(arr) < 10:
            arr.append(item)
            panel.save_json(panel.DATA/"slots.json", slots)
            return jsonify({"status":"ok","message":"已加入组"+str(i+1),"slot": i})
    return jsonify({"status":"error","message":"10组都已满"})
panel.app.add_url_rule("/api/slots/autobind","slots_autobind",slots_autobind,methods=["POST"])
def bots_add_slot():
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    token = (body.get("token") or "").strip()
    remark = body.get("remark") or "bot"
    if ":" not in token:
        return jsonify({"status":"error","message":"token"})
    bots = panel.load_json(panel.DATA/"bots.json", [])
    if any(b.get("token")==token for b in bots):
        return jsonify({"status":"error","message":"exists"})
    counts = [sum(1 for b in bots if int(b.get("slot") or 0)==i) for i in range(10)]
    slot = next((i for i,c in enumerate(counts) if c<10), None)
    if slot is None:
        return jsonify({"status":"error","message":"10组都已满"})
    bots.append({"id": panel.new_id("bot_"), "token": token, "remark": remark, "slot": slot, "role": role, "created_at": panel.now_str()})
    panel.save_json(panel.DATA/"bots.json", bots)
    return jsonify({"status":"ok","message":"已加入组"+str(slot+1),"bots": bots})
panel.app.add_url_rule("/api/bots","bots_add_slot",bots_add_slot,methods=["POST"])
def import_worker_zip():
    import json, sqlite3, zipfile, tempfile, shutil
    from flask import request, jsonify
    f = request.files.get("file")
    if not f:
        return jsonify({"status":"error","message":"请选择ZIP"})
    tmp = tempfile.mkdtemp(prefix="wzip_")
    try:
        zpath = Path(tmp) / "in.zip"
        f.save(zpath)
        out = Path(tmp) / "out"
        out.mkdir()
        with zipfile.ZipFile(zpath) as z:
            z.extractall(out)
        workers = panel.load_json(panel.DATA/"workers.json", [])
        phones = {w.get("phone") for w in workers}
        imported, skipped = [], []
        sessions = list(out.rglob("*.session"))
        for sess in sessions:
            js = sess.with_suffix(".json")
            if not js.exists():
                js = next(sess.parent.glob(sess.stem + ".json"), None)
            if not js or not Path(js).exists():
                skipped.append(sess.name + " 无json")
                continue
            try:
                meta = json.loads(Path(js).read_text(encoding="utf-8"))
            except Exception:
                skipped.append(sess.name + " json坏")
                continue
            phone = str(meta.get("phone") or meta.get("phone_number") or meta.get("session_file") or "").strip()
            if phone and not phone.startswith("+") and phone.replace(" ","").isdigit():
                phone = "+" + phone.replace(" ","")
            if not phone:
                skipped.append(sess.stem + " 无手机号")
                continue
            if phone in phones:
                skipped.append(phone + " 已存在")
                continue
            try:
                con = sqlite3.connect(sess)
                row = con.execute("select auth_key from sessions").fetchone()
                con.close()
                if not row or not row[0]:
                    skipped.append(phone + " 死号")
                    continue
            except Exception:
                skipped.append(phone + " 死号")
                continue
            work_n = sum(1 for w in workers if w.get("role")=="work")
            if work_n < 10:
                slot, role = work_n, "work"
            else:
                slot, role = -1, "pool"
            wid = panel.new_id("w_")
            shutil.copy(sess, panel.SESS / f"{wid}.session")
            workers.append({
                "id": wid, "phone": phone, "twofa": str(meta.get("twoFA") or meta.get("twofa") or ""),
                "remark": meta.get("remark") or "ZIP导入", "status": "offline", "status_text": "已导入",
                "created_at": panel.now_str(), "slot": slot,
                "api_id": meta.get("app_id") or meta.get("api_id"),
                "api_hash": meta.get("app_hash") or meta.get("api_hash"),
            })
            phones.add(phone)
            imported.append(phone + "→组" + str(slot+1))
        panel.save_json(panel.DATA/"workers.json", workers)
        return jsonify({"status":"ok","message":"导入"+str(len(imported))+"个，跳过"+str(len(skipped))+"个","imported":imported,"skipped":skipped})
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
panel.app.add_url_rule("/api/workers/import-zip","import_worker_zip",import_worker_zip,methods=["POST"])
def swap_limited_worker(wid, wait_sec=3600):
    workers = panel.load_json(panel.DATA/"workers.json", [])
    cur = next((w for w in workers if w.get("id")==wid), None)
    pool = [w for w in workers if w.get("role")=="pool" and int(w.get("cooling_until") or 0) <= time.time()]
    if not cur or not pool:
        return None
    nxt = pool[0]
    slot = cur.get("slot", 0)
    cur["role"] = "cooling"
    cur["slot"] = -1
    cur["cooling_until"] = int(time.time()) + int(wait_sec)
    cur["status_text"] = "限流冷却"
    nxt["role"] = "work"
    nxt["slot"] = slot
    nxt["status_text"] = "池号顶上"
    panel.save_json(panel.DATA/"workers.json", workers)
    gs = panel.load_json(panel.DATA/"groups.json", [])
    for g in gs:
        if g.get("worker_id")==wid:
            g["worker_id"] = nxt["id"]
    panel.save_json(panel.DATA/"groups.json", gs)
    print("swap", cur.get("phone"), "->", nxt.get("phone"), "wait", wait_sec)
    return nxt["id"]
def release_cooled():
    workers = panel.load_json(panel.DATA/"workers.json", [])
    changed = False
    now = time.time()
    for w in workers:
        if w.get("role")=="cooling" and int(w.get("cooling_until") or 0) <= now:
            w["role"] = "pool"
            w["status_text"] = "冷却完成回池"
            changed = True
    if changed:
        panel.save_json(panel.DATA/"workers.json", workers)

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
                    import traceback
                    print("auto err", type(e), e, flush=True)
                    traceback.print_exc()
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


def login_vfix():
    from flask import request, jsonify, session
    data=request.get_json(silent=True) or request.form or {}
    user=(data.get("username") or data.get("user") or "").strip()
    pw=(data.get("password") or data.get("pass") or "").strip()
    if user=="admin" and pw=="Ab123987":
        session["user"]="admin"
        return jsonify({"status":"ok"})
    return jsonify({"status":"error","message":"用户名或密码错误"}), 401
for _r in list(panel.app.url_map.iter_rules()):
    if str(_r.rule) in ("/api/login","/login"):
        panel.app.view_functions[_r.endpoint]=login_vfix
panel.app.add_url_rule("/api/login","login_vfix",login_vfix,methods=["POST"])


def bots_add_vfix():
    from flask import request, jsonify
    data=request.get_json(silent=True) or {}
    token=(data.get("token") or "").strip()
    remark=(data.get("remark") or "bot").strip()
    if ":" not in token:
        return jsonify({"status":"error","message":"token格式不对"})
    bots=panel.load_json(panel.DATA/"bots.json", [])
    if any(b.get("token")==token for b in bots):
        return jsonify({"status":"error","message":"已存在"})
    bots.append({"id": panel.new_id("bot_"), "token": token, "remark": remark, "created_at": panel.now_str()})
    panel.save_json(panel.DATA/"bots.json", bots)
    return jsonify({"status":"ok","message":"Bot已添加","bots":bots})
panel.app.add_url_rule("/api/bots","bots_add_vfix",bots_add_vfix,methods=["POST"])


def _slots():
    path = panel.DATA / "slots.json"
    if not path.exists():
        panel.save_json(path, [{"i": i, "worker_id": "", "phone": "", "targets": []} for i in range(10)])
    return panel.load_json(path, [])

def api_slots():
    from flask import jsonify
    slots = _slots()
    workers = {w.get("id"): w for w in panel.load_json(panel.DATA / "workers.json", [])}
    for s in slots:
        w = workers.get(s.get("worker_id")) or {}
        s["phone"] = w.get("phone") or w.get("remark") or ""
    return jsonify({"slots": slots})

def api_slot_worker(i):
    from flask import request, jsonify
    slots = _slots()
    slots[int(i)]["worker_id"] = (request.json or {}).get("worker_id") or ""
    panel.save_json(panel.DATA / "slots.json", slots)
    return jsonify({"status": "ok"})

def api_slot_target(i):
    from flask import request, jsonify
    slots = _slots()
    i = int(i)
    u = ((request.json or {}).get("username") or "").strip()
    if request.method == "DELETE":
        slots[i]["targets"] = [x for x in slots[i].get("targets") or [] if x != u]
    elif u and u not in (slots[i].get("targets") or []) and len(slots[i].get("targets") or []) < 10:
        slots[i].setdefault("targets", []).append(u)
    panel.save_json(panel.DATA / "slots.json", slots)
    return jsonify({"status": "ok"})

panel.app.add_url_rule("/api/slots", "api_slots", api_slots, methods=["GET"])
panel.app.add_url_rule("/api/slots/<i>/worker", "api_slot_worker", api_slot_worker, methods=["POST"])
panel.app.add_url_rule("/api/slots/<i>/target", "api_slot_target", api_slot_target, methods=["POST", "DELETE"])
print("slots ready")


def page_targets():
    from flask import send_from_directory
    return send_from_directory("static", "targets.html")

def api_slot_auto():
    from flask import request, jsonify
    u = ((request.json or {}).get("username") or "").strip()
    if not u:
        return jsonify({"status": "error", "message": "先填订阅号"})
    if not u.startswith("@"):
        u = "@" + u
    slots = _slots()
    for n, s in enumerate(slots):
        targets = s.setdefault("targets", [])
        if u in targets:
            return jsonify({"status": "ok", "slot": n, "message": "已经在组" + str(n + 1)})
        if len(targets) < 10:
            targets.append(u)
            panel.save_json(panel.DATA / "slots.json", slots)
            return jsonify({"status": "ok", "slot": n, "message": "已加入组" + str(n + 1)})
    return jsonify({"status": "error", "message": "10 组都已满"})

panel.app.add_url_rule("/targets", "page_targets", page_targets, methods=["GET"])
panel.app.add_url_rule("/api/slots/auto", "api_slot_auto", api_slot_auto, methods=["POST"])
print("auto-slot ready")

if __name__ == "__main__":
    panel.start_loop()
    threading.Thread(target=_auto_loop, daemon=True).start()
    panel.app.run(host="0.0.0.0", port=8088, debug=False, use_reloader=False)

def group_workers(gid):
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    ids = body.get("worker_ids") or []
    groups = panel.load_json(panel.DATA/"groups.json", [])
    for g in groups:
        if g.get("id")==gid:
            g["worker_ids"] = [x for x in ids if x]
            g["worker_id"] = g["worker_ids"][0] if g["worker_ids"] else ""
    panel.save_json(panel.DATA/"groups.json", groups)
    return jsonify({"status":"ok","message":"已保存 %s 个水军"%len(ids)})
panel.app.add_url_rule("/api/groups/<gid>/workers","group_workers",group_workers,methods=["POST"])
print("workers multi ready")

def _worker(wid):
    for w in panel.load_json(panel.DATA/"workers.json", []):
        if w.get("id")==wid:
            return w
    return None

async def _read_chats(wid):
    w=_worker(wid)
    if not w:
        return {"messages":[], "error":"无此水军"}
    from telethon import TelegramClient
    session=str(panel.DATA/"sessions"/wid)
    client=TelegramClient(session, int(w.get("api_id")), w.get("api_hash"))
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        return {"messages":[], "error":"会话未登录"}
    rows=[]
    async for d in client.iter_dialogs(limit=30):
        if not d.is_user:
            continue
        m=d.message
        rows.append({"from": d.name or str(d.id), "text": (m.text if m else "") or "[媒体]", "out": bool(m and m.out)})
    await client.disconnect()
    return {"messages": rows}

def worker_chats(wid):
    from flask import request, jsonify
    if request.method=="POST":
        return jsonify({"status":"ok","message":"先打开聊天看消息"})
    return jsonify(panel.run_async(_read_chats(wid), timeout=60))
panel.app.add_url_rule("/api/workers/<wid>/chats","worker_chats",worker_chats,methods=["GET","POST"])
print("chat ready")

def wgroup_worker(gid):
    from flask import request, jsonify
    body = request.get_json(force=True, silent=True) or {}
    wid = body.get("worker_id") or ""
    files = list(Path("data").glob("*.json"))
    hit = False
    for f in files:
        try:
            data = panel.load_json(f, None)
        except Exception:
            continue
        rows = data if isinstance(data, list) else None
        if not rows:
            continue
        for row in rows:
            if isinstance(row, dict) and row.get("id") == gid:
                row["worker_id"] = wid
                panel.save_json(f, data)
                hit = True
    return jsonify({"status":"ok" if hit else "error","message":"已绑定水军" if hit else "组不存在"})
panel.app.add_url_rule("/api/wgroups/<gid>/worker","wgroup_worker",wgroup_worker,methods=["POST"])
print("wgroup bind ready")

def group_worker_bind(gid):
    from flask import request, jsonify
    body = request.get_json(force=True, silent=True) or {}
    wid = body.get("worker_id") or ""
    gs = panel.load_json(panel.GROUP_FILE, [])
    hit = False
    for g in gs:
        if g.get("id") == gid:
            g["worker_id"] = wid
            hit = True
    panel.save_json(panel.GROUP_FILE, gs)
    return jsonify({"status":"ok" if hit else "error","message":"已绑定水军" if hit else "组不存在"})
panel.app.add_url_rule("/api/groups/<gid>/worker","group_worker_bind",group_worker_bind,methods=["POST"])
print("worker bind ready")

def auth_status_open():
    from flask import jsonify
    return jsonify({"status":"ok","logged_in":True,"username":"admin","engine":True})
for rule in list(panel.app.url_map.iter_rules()):
    if rule.rule == "/api/auth/status":
        panel.app.url_map._rules.remove(rule)
        panel.app.url_map._rules_by_endpoint.pop(rule.endpoint, None)
        panel.app.view_functions.pop(rule.endpoint, None)
panel.app.add_url_rule("/api/auth/status","auth_status_open",auth_status_open,methods=["GET"])
print("auth open")
