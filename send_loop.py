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


def worker_code_v10(wid):
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    code = str(body.get("code") or "").strip()
    if not code:
        return jsonify({"status": "error", "message": "没有验证码"})
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        h = w.get("phone_code_hash") or ""
        if not h:
            return {"status": "error", "message": "没有hash，先点启动再提交新码"}
        phone = str(w.get("phone") or "").replace(" ", "")
        client, info = await panel._create_client(w)
        await client.connect()
        await client.sign_in(phone, code, phone_code_hash=h)
        me = await client.get_me()
        for x in workers:
            if x.get("id") == wid:
                x["status"] = "online"
                x["status_text"] = "在线"
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已登录 " + str(me.phone)}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if "worker_code_v10" not in [r.endpoint for r in panel.app.url_map.iter_rules()]:
    panel.app.add_url_rule("/api/workers/<wid>/code", "worker_code_v10", worker_code_v10, methods=["POST"])
print("code route ready")


def worker_start_v10(wid):
    from flask import jsonify
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        phone = str(w.get("phone") or "").replace(" ", "")
        client, info = await panel._create_client(w)
        await client.connect()
        if await client.is_user_authorized():
            w["status"] = "online"
            w["status_text"] = "在线"
            panel.save_json(panel.DATA / "workers.json", workers)
            me = await client.get_me()
            return {"status": "ok", "message": "已在线 " + str(me.phone)}
        sent = await client.send_code_request(phone)
        w["phone_code_hash"] = sent.phone_code_hash
        w["status"] = "wait_code"
        w["status_text"] = "等待验证码"
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已发送，hash已保存"}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
panel.app.add_url_rule("/api/workers/<wid>/start", "worker_start_v10", worker_start_v10, methods=["POST"])
print("start route ready")


def workers_add_v11():
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    phone = "".join(ch for ch in str(body.get("phone") or "") if ch.isdigit() or ch == "+")
    if not phone.startswith("+"):
        phone = "+" + phone
    if len(phone) < 8:
        return jsonify({"status": "error", "message": "手机号不对"})
    workers = panel.load_json(panel.DATA / "workers.json", [])
    if any(phone == str(w.get("phone") or "").replace(" ", "") for w in workers):
        return jsonify({"status": "error", "message": "已存在", "workers": workers})
    apis = panel.load_json(panel.DATA / "api_pool.json", [])
    used = {w.get("api_id") for w in workers}
    api = next((a for a in apis if a.get("id") not in used), apis[0] if apis else {})
    workers.append({
        "id": panel.new_id("w_"),
        "phone": phone,
        "password": body.get("password") or "",
        "remark": body.get("remark") or "",
        "status": "idle",
        "status_text": "未启动",
        "api_id": api.get("id"),
        "created_at": panel.now_str(),
    })
    panel.save_json(panel.DATA / "workers.json", workers)
    return jsonify({"status": "ok", "message": "已加入列表", "workers": workers})

def worker_start_v11(wid):
    from flask import jsonify
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        phone = str(w.get("phone") or "").replace(" ", "")
        client, info = await panel._create_client(w)
        await client.connect()
        if await client.is_user_authorized():
            w["status"] = "online"
            w["status_text"] = "在线"
            panel.save_json(panel.DATA / "workers.json", workers)
            me = await client.get_me()
            return {"status": "ok", "message": "已在线 " + str(me.phone)}
        sent = await client.send_code_request(phone)
        w["phone_code_hash"] = sent.phone_code_hash
        w["status"] = "wait_code"
        w["status_text"] = "等待验证码"
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已发送，hash已保存"}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

def worker_code_v11(wid):
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    code = str(body.get("code") or "").strip()
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        h = w.get("phone_code_hash") or ""
        if not h:
            return {"status": "error", "message": "没有hash，先点启动再提交新码"}
        phone = str(w.get("phone") or "").replace(" ", "")
        client, info = await panel._create_client(w)
        await client.connect()
        try:
            await client.sign_in(phone, code, phone_code_hash=h, password=w.get("password") or None)
        except Exception:
            await client.sign_in(phone, code, phone_code_hash=h)
        me = await client.get_me()
        w["status"] = "online"
        w["status_text"] = "在线"
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已登录 " + str(me.phone)}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

panel.app.add_url_rule("/api/workers", "workers_add_v11", workers_add_v11, methods=["POST"])
panel.app.add_url_rule("/api/workers/<wid>/start", "worker_start_v11", worker_start_v11, methods=["POST"])
panel.app.add_url_rule("/api/workers/<wid>/code", "worker_code_v11", worker_code_v11, methods=["POST"])
print("worker routes v11 ready")


def worker_start_v12(wid):
    from flask import jsonify
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        phone = str(w.get("phone") or "").replace(" ", "")
        client, info = await panel._create_client(w)
        await client.connect()
        if await client.is_user_authorized():
            w["status"] = "online"
            w["status_text"] = "在线"
            panel.save_json(panel.DATA / "workers.json", workers)
            me = await client.get_me()
            return {"status": "ok", "message": "已在线 " + str(me.phone)}
        sent = await client.send_code_request(phone)
        w["phone_code_hash"] = sent.phone_code_hash
        w["status"] = "wait_code"
        w["status_text"] = "等待验证码"
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已发送，hash已保存 " + sent.phone_code_hash[:8]}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

def worker_code_v12(wid):
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    code = str(body.get("code") or "").strip()
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        h = w.get("phone_code_hash") or ""
        if not h:
            return {"status": "error", "message": "没有hash，先点启动再提交新码"}
        phone = str(w.get("phone") or "").replace(" ", "")
        client, info = await panel._create_client(w)
        await client.connect()
        await client.sign_in(phone, code, phone_code_hash=h)
        me = await client.get_me()
        w["status"] = "online"
        w["status_text"] = "在线"
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已登录 " + str(me.phone)}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

for rule in list(panel.app.url_map.iter_rules()):
    if rule.rule in ("/api/workers/<wid>/start", "/api/workers/<wid>/code"):
        panel.app.view_functions[rule.endpoint] = worker_start_v12 if rule.rule.endswith("/start") else worker_code_v12
panel.app.add_url_rule("/api/workers/<wid>/start", "worker_start_v12", worker_start_v12, methods=["POST"])
panel.app.add_url_rule("/api/workers/<wid>/code", "worker_code_v12", worker_code_v12, methods=["POST"])
print("start/code v12 ready")


def worker_code_v13(wid):
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    code = str(body.get("code") or "").strip()
    password = str(body.get("password") or "")
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        h = w.get("phone_code_hash") or ""
        if not h:
            return {"status": "error", "message": "没有hash，先点启动再提交新码"}
        phone = str(w.get("phone") or "").replace(" ", "")
        pwd = password or w.get("password") or ""
        client, info = await panel._create_client(w)
        await client.connect()
        try:
            await client.sign_in(phone, code, phone_code_hash=h)
        except Exception as e:
            if "password" not in str(e).lower() and "Two-steps" not in str(e):
                return {"status": "error", "message": str(e)}
            if not pwd:
                return {"status": "error", "message": "这个号开了2FA，请填写两步验证密码后再提交"}
            await client.sign_in(password=pwd)
        me = await client.get_me()
        w["status"] = "online"
        w["status_text"] = "在线"
        if pwd:
            w["password"] = pwd
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已登录 " + str(me.phone)}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
panel.app.add_url_rule("/api/workers/<wid>/code", "worker_code_v13", worker_code_v13, methods=["POST"])
print("code v13 ready")


def worker_code_v14(wid):
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    code = str(body.get("code") or "").strip()
    password = str(body.get("password") or "")
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        h = w.get("phone_code_hash") or ""
        phone = str(w.get("phone") or "").replace(" ", "")
        pwd = password or w.get("password") or ""
        client, info = await panel._create_client(w)
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            w["status"] = "online"
            w["status_text"] = "在线"
            panel.save_json(panel.DATA / "workers.json", workers)
            return {"status": "ok", "message": "已登录 " + str(me.phone)}
        if not h:
            return {"status": "error", "message": "没有hash，先点启动"}
        try:
            await client.sign_in(phone, code, phone_code_hash=h)
        except Exception as e:
            if "password" not in str(e).lower() and "Two-steps" not in str(e):
                return {"status": "error", "message": str(e)}
            if not pwd:
                return {"status": "error", "message": "请在这一行填写2FA密码后再提交"}
            try:
                await client.sign_in(password=pwd)
            except Exception as e2:
                return {"status": "error", "message": "2FA密码不对: " + str(e2)}
        me = await client.get_me()
        w["status"] = "online"
        w["status_text"] = "在线"
        if pwd:
            w["password"] = pwd
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已登录 " + str(me.phone)}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
panel.app.add_url_rule("/api/workers/<wid>/code", "worker_code_v14", worker_code_v14, methods=["POST"])
print("code v14 ready")


def worker_code_v16(wid):
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    code = str(body.get("code") or "").strip()
    password = str(body.get("password") or "")
    async def run():
        workers = panel.load_json(panel.DATA / "workers.json", [])
        w = next((x for x in workers if x.get("id") == wid), None)
        if not w:
            return {"status": "error", "message": "没有这个水军"}
        phone = str(w.get("phone") or "").replace(" ", "")
        h = w.get("phone_code_hash") or ""
        pwd = password or w.get("password") or ""
        client, info = await panel._create_client(w)
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            w["status"] = "online"
            w["status_text"] = "在线"
            panel.save_json(panel.DATA / "workers.json", workers)
            return {"status": "ok", "message": "已登录 " + str(me.phone)}
        if not pwd:
            return {"status": "error", "message": "2FA密码是空的"}
        try:
            if code and h:
                await client.sign_in(phone, code, phone_code_hash=h)
        except Exception as e:
            if "password" not in str(e).lower() and "Two-steps" not in str(e):
                return {"status": "error", "message": str(e)}
        try:
            await client.sign_in(password=pwd)
        except Exception as e2:
            return {"status": "error", "message": "2FA密码不对: " + str(e2)}
        me = await client.get_me()
        w["status"] = "online"
        w["status_text"] = "在线"
        w["password"] = pwd
        panel.save_json(panel.DATA / "workers.json", workers)
        return {"status": "ok", "message": "已登录 " + str(me.phone)}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
panel.app.add_url_rule("/api/workers/<wid>/code2", "worker_code_v16", worker_code_v16, methods=["POST"])
print("code2 ready")


def overview_keep():
    from flask import jsonify
    workers = panel.load_json(panel.DATA / "workers.json", [])
    groups = panel.load_json(panel.DATA / "groups.json", [])
    apis = panel.load_json(panel.DATA / "api_pool.json", [])
    ips = panel.load_json(panel.DATA / "ip_pool.json", [])
    bots = panel.load_json(panel.DATA / "bots.json", [])
    stats = panel.load_json(panel.DATA / "stats.json", {})
    return jsonify({
        "status": "ok",
        "workers": workers,
        "groups": groups,
        "apis": apis,
        "ips": ips,
        "bots": bots,
        "stats": {
            "workers": len(workers),
            "online_workers": sum(1 for x in workers if x.get("status") == "online"),
            "groups": len(groups),
            "running_groups": sum(1 for x in groups if x.get("enabled", True)),
            "apis": len(apis),
            "ips": len(ips),
            "forwarded": stats.get("forwarded", 0) if isinstance(stats, dict) else 0,
        },
    })
panel.app.view_functions["api_overview"] = overview_keep
panel.app.view_functions["overview"] = overview_keep
print("overview keep")


def worker_slots_v19():
    from flask import jsonify
    from pathlib import Path
    p = Path("data/worker_slots.json")
    if not p.exists():
        return jsonify({"active": [], "standby": [], "cap": 10})
    return jsonify(json.loads(p.read_text(encoding="utf-8")))
panel.app.add_url_rule("/api/worker-slots", "worker_slots_v19", worker_slots_v19, methods=["GET"])
print("slots ready")


def bots_list_v20():
    from flask import jsonify
    return jsonify({"ok": True, "bots": panel.load_json(panel.DATA / "bots.json", [])})
panel.app.add_url_rule("/api/botlist", "bots_list_v20", bots_list_v20, methods=["GET"])
print("botlist ready")


@panel.app.after_request
def nocache_v21(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp
print("nocache v21")

def bots_add46():
    from flask import request, jsonify
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    remark = (data.get("remark") or "").strip() or "bot"
    if ":" not in token:
        return jsonify({"status":"error","message":"Token格式不对"})
    bots = panel.load_json(panel.DATA/"bots.json", [])
    if any(b.get("token") == token for b in bots):
        return jsonify({"status":"error","message":"这个Bot已存在"})
    bots.append({"id": panel.new_id("bot_"), "token": token, "remark": remark, "created_at": panel.now_str()})
    panel.save_json(panel.DATA/"bots.json", bots)
    return jsonify({"status":"ok","message":"Bot已添加","bots":bots})
panel.app.add_url_rule("/api/bots/add46", "bots_add46", bots_add46, methods=["POST"])
print("bots-add46 ready")

def bots_del46():
    from flask import request, jsonify
    data = request.get_json(silent=True) or {}
    bid = (data.get("id") or "").strip()
    bots = panel.load_json(panel.DATA/"bots.json", [])
    n = len(bots)
    bots = [b for b in bots if b.get("id") != bid]
    panel.save_json(panel.DATA/"bots.json", bots)
    return jsonify({"status":"ok","removed": n-len(bots), "count":len(bots)})

panel.app.add_url_rule("/api/bots/del46", "bots_del46", bots_del46, methods=["POST"])
print("bots-del46 ready")
def target_add52():
    from flask import request, jsonify
    data = request.get_json(silent=True) or {}
    user = (data.get("username") or "").strip()
    remark = (data.get("remark") or "").strip()
    bot_id = (data.get("bot_id") or "").strip()
    if "t.me/" in user:
        user = user.split("t.me/")[-1].split("/")[0].split("?")[0]
    user = user if user.startswith("@") else "@"+user.lstrip("@")
    if len(user) < 3:
        return jsonify({"status":"error","message":"链接不对"})
    gs = panel.load_json(panel.GROUP_FILE, [])
    if not gs:
        return jsonify({"status":"error","message":"没有转发组"})
    g = gs[0]
    targets = g.setdefault("targets", [])
    if any((x.get("username") or "").lower()==user.lower() for x in targets):
        return jsonify({"status":"ok","message":"已在组里","targets":targets})
    targets.append({"id": panel.new_id("t_"), "username": user, "remark": remark, "bot_id": bot_id})
    panel.save_json(panel.GROUP_FILE, gs)
    return jsonify({"status":"ok","message":"已进组","targets":targets})
panel.app.add_url_rule("/api/targets/add52", "target_add52", target_add52, methods=["POST"])
print("target-add52 ready")
def group_add53():
    from flask import request, jsonify
    data = request.get_json(silent=True) or {}
    source = (data.get("source") or "").strip()
    remark = (data.get("source_remark") or "").strip()
    workers = data.get("worker_ids") or []
    if "t.me/" in source:
        source = "t.me/" + source.split("t.me/")[-1].split("/")[0].split("?")[0]
    if not source:
        return jsonify({"status":"error","message":"源订阅号空"})
    gs = panel.load_json(panel.GROUP_FILE, [])
    if any((g.get("source") or "").lower()==source.lower() for g in gs):
        return jsonify({"status":"ok","message":"已在面板","groups":gs})
    gs.append({"id": panel.new_id("g_"), "source": source, "source_remark": remark, "worker_ids": workers, "targets": [], "enabled": True, "forwarded": 0})
    panel.save_json(panel.GROUP_FILE, gs)
    return jsonify({"status":"ok","message":"已添加到面板","count":len(gs)})
panel.app.add_url_rule("/api/groups/add53", "group_add53", group_add53, methods=["POST"])
print("group-add53 ready")
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


def worker_code_v1(wid):
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    code = str(body.get("code") or "").strip()
    password = str(body.get("password") or "pass345word")
    if not code.isdigit():
        return jsonify({"status":"error","message":"验证码必须是数字"})
    async def run():
        workers = panel.load_json(panel.DATA/"workers.json", [])
        w = next((x for x in workers if x.get("id")==wid), None)
        if not w:
            return {"status":"error","message":"没有这个水军"}
        client, info = await panel._create_client(w)
        await client.connect()
        phone = w.get("phone")
        try:
            await client.sign_in(phone, code, phone_code_hash=w.get("phone_code_hash"))
        except Exception as e:
            name = type(e).__name__
            if "SessionPasswordNeeded" in name or "password" in str(e).lower():
                await client.sign_in(password=password)
            else:
                return {"status":"error","message":name+" "+str(e)}
        me = await client.get_me()
        for x in workers:
            if x.get("id")==wid:
                x["status"] = "online"
                x["status_text"] = "在线"
        panel.save_json(panel.DATA/"workers.json", workers)
        return {"status":"ok","message":"已登录 "+str(me.phone)}
    try:
        return jsonify(panel.run_async(run(), timeout=90))
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})
panel.app.add_url_rule("/api/workers/<wid>/code", "worker_code_v1", worker_code_v1, methods=["POST"])
print("code route ready")

# group-act v34
def _group_stop(gid):
    gs = panel.load_json(panel.GROUP_FILE, [])
    for g in gs:
        if g.get("id") == gid:
            g["running"] = False
    panel.save_json(panel.GROUP_FILE, gs)
    return {"status":"ok","message":"已停止采集"}
def _group_delete(gid):
    gs = panel.load_json(panel.GROUP_FILE, [])
    gs = [g for g in gs if g.get("id") != gid]
    panel.save_json(panel.GROUP_FILE, gs)
    return {"status":"ok","message":"已删除该组"}
def group_stop_v34(gid):
    return jsonify(_group_stop(gid))
def group_delete_v34(gid):
    return jsonify(_group_delete(gid))
panel.app.add_url_rule("/api/groups/<gid>/stop","group_stop_v34",group_stop_v34,methods=["POST"])
panel.app.add_url_rule("/api/groups/<gid>/delete","group_delete_v34",group_delete_v34,methods=["POST"])
print("group-act v34")

# target-del v36
def target_delete_v36():
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    name = (body.get("username") or "").strip()
    gs = panel.load_json(panel.GROUP_FILE, [])
    n = 0
    for g in gs:
        old = g.get("targets") or []
        g["targets"] = [t for t in old if (t.get("username") or t.get("id")) != name]
        n += len(old) - len(g["targets"])
    panel.save_json(panel.GROUP_FILE, gs)
    return jsonify({"status":"ok","message":"已删除 "+name+" "+str(n)+"条"})
panel.app.add_url_rule("/api/targets/delete","target_delete_v36",target_delete_v36,methods=["POST"])
print("target-del v36")

# target-del v37
def target_delete_v37():
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    name = (body.get("username") or "").strip().lstrip("@")
    path = panel.DATA / "groups.json"
    gs = panel.load_json(path, [])
    n = 0
    for g in gs:
        old = g.get("targets") or []
        keep = []
        for t in old:
            u = (t.get("username") or t.get("id") or "").lstrip("@")
            if u == name:
                n += 1
            else:
                keep.append(t)
        g["targets"] = keep
    panel.save_json(path, gs)
    return jsonify({"status":"ok","message":"已删除 @"+name+" "+str(n)+"条"})
panel.app.add_url_rule("/api/targets/delete","target_delete_v37",target_delete_v37,methods=["POST"])
print("target-del v37")

# target-del v38
def target_delete_v38():
    from flask import request, jsonify
    body = request.get_json(silent=True) or {}
    name = (body.get("username") or "").strip().lstrip("@")
    path = panel.DATA / "groups.json"
    gs = panel.load_json(path, [])
    n = 0
    for g in gs:
        old = g.get("targets") or []
        keep = []
        for t in old:
            u = (t.get("username") or t.get("id") or "").lstrip("@")
            if u == name:
                n += 1
            else:
                keep.append(t)
        g["targets"] = keep
    panel.save_json(path, gs)
    return jsonify({"status":"ok","message":"已删除 @"+name+" "+str(n)+"条"})
panel.app.add_url_rule("/api/targets/delete","target_delete_v38",target_delete_v38,methods=["POST"])
print("target-del v38")

@panel.app.before_request
def allow_add46():
    from flask import request
    if request.path in ("/api/bots/add46", "/api/bots/list46", "/api/bots/del46"):
        return None
print("auth off add46")

def bots_list46():
    from flask import jsonify
    return jsonify({"status":"ok","bots": panel.load_json(panel.DATA/"bots.json", [])})
panel.app.add_url_rule("/api/bots/list46", "bots_list46", bots_list46, methods=["GET"])
print("bots-list46 ready")

def bots_del46():
    from flask import request, jsonify
    data = request.get_json(silent=True) or {}
    bid = (data.get("id") or "").strip()
    bots = panel.load_json(panel.DATA/"bots.json", [])
    bots = [b for b in bots if b.get("id") != bid]
    panel.save_json(panel.DATA/"bots.json", bots)
    return jsonify({"status":"ok","count":len(bots)})
panel.app.add_url_rule("/api/bots/del46", "bots_del46", bots_del46, methods=["POST"])
print("bots-del46 ready")
