#!/usr/bin/env python3
"""TG 订阅转发 Pro — 1 源订阅转发到多个目标订阅。"""
from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request, send_from_directory, session

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
SESS = BASE / "sessions"
STATIC = BASE / "static"
for p in (DATA, SESS, STATIC, BASE / "logs"):
    p.mkdir(parents=True, exist_ok=True)

API_FILE = DATA / "api_pool.json"
IP_FILE = DATA / "ip_pool.json"
WORKER_FILE = DATA / "workers.json"
GROUP_FILE = DATA / "groups.json"
LOG_FILE = DATA / "logs.json"
STATS_FILE = DATA / "stats.json"

ADMIN_USERNAME = os.environ.get("TG_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("TG_ADMIN_PASS", "Ab123456987")

app = Flask(__name__, static_folder=str(STATIC), static_url_path="/static")
app.secret_key = os.environ.get("TG_SECRET_KEY", "tg-forward-pro-secret-key")

_lock = threading.Lock()
_loop: Optional[asyncio.AbstractEventLoop] = None
_clients: Dict[str, Any] = {}
_pending: Dict[str, Any] = {}
_handlers: Dict[str, Any] = {}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_json(path: Path, default):
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def log(msg: str, level: str = "info") -> None:
    with _lock:
        logs = load_json(LOG_FILE, [])
        logs.append({"time": now_str(), "level": level, "msg": msg})
        save_json(LOG_FILE, logs[-500:])
    print(f"[{level}] {msg}", flush=True)


def bump_forwarded(n: int = 1) -> None:
    with _lock:
        st = load_json(STATS_FILE, {"forwarded": 0})
        st["forwarded"] = int(st.get("forwarded", 0)) + n
        save_json(STATS_FILE, st)


def new_id(prefix: str = "") -> str:
    return prefix + uuid.uuid4().hex[:10]


# --------------- parse helpers ---------------
def parse_api_lines(text: str) -> List[dict]:
    items = []
    for raw in (text or "").splitlines():
        line = raw.strip().replace("，", ",")
        if not line or line.startswith("#"):
            continue
        parts = re.split(r"[\s,]+", line)
        if len(parts) < 2:
            continue
        api_id, api_hash = parts[0].strip(), parts[1].strip()
        if not api_id.isdigit() or len(api_hash) < 16:
            continue
        items.append({"api_id": int(api_id), "api_hash": api_hash})
    return items


def parse_proxy_line(line: str) -> Optional[dict]:
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    ptype = "socks5"
    user = pwd = None
    host = None
    port = None
    m = re.match(r"^(socks5|socks4|http|https)://(?:([^:@]+):([^@]+)@)?([^:/]+):(\d+)", s, re.I)
    if m:
        ptype = m.group(1).lower().replace("https", "http")
        user, pwd, host, port = m.group(2), m.group(3), m.group(4), int(m.group(5))
    else:
        m2 = re.match(r"^([^:/]+):(\d+)(?::([^:]+):(.+))?$", s)
        if not m2:
            return None
        host, port = m2.group(1), int(m2.group(2))
        user, pwd = m2.group(3), m2.group(4)
    display = f"{ptype}://{host}:{port}"
    if user:
        display = f"{ptype}://{user}:***@{host}:{port}"
    return {
        "id": new_id("ip_"),
        "type": ptype,
        "host": host,
        "port": port,
        "username": user or "",
        "password": pwd or "",
        "display": display,
        "raw": s,
        "worker_ids": [],
    }


def telethon_proxy(ip: dict):
    if not ip:
        return None
    import socks

    kind = {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4, "http": socks.HTTP}.get(ip.get("type"), socks.SOCKS5)
    user = ip.get("username") or None
    pwd = ip.get("password") or None
    return (kind, ip["host"], int(ip["port"]), True, user, pwd)


def normalize_entity(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    s = s.split("?")[0].strip("/")
    if s.startswith("@"):
        return s
    if re.fullmatch(r"-?\d+", s):
        return s
    return "@" + s if s else s


# --------------- asyncio loop ---------------
def start_loop() -> None:
    global _loop
    _loop = asyncio.new_event_loop()

    def _run():
        asyncio.set_event_loop(_loop)
        _loop.run_forever()

    t = threading.Thread(target=_run, daemon=True)
    t.start()


def run_async(coro, timeout: float = 60):
    if _loop is None:
        raise RuntimeError("event loop not started")
    fut = asyncio.run_coroutine_threadsafe(coro, _loop)
    return fut.result(timeout=timeout)


# --------------- assignment ---------------
def find_worker_proxy(worker_id: str) -> Optional[dict]:
    ips = load_json(IP_FILE, [])
    for ip in ips:
        if worker_id in (ip.get("worker_ids") or []):
            return ip
    return None


def pick_idle_api(worker_id: str) -> Optional[dict]:
    """1 水军自动配置 1 个空闲 API。已绑定自己的则复用。"""
    with _lock:
        apis = load_json(API_FILE, [])
        workers = load_json(WORKER_FILE, [])
        occupied = {w.get("api_key") for w in workers if w.get("id") != worker_id and w.get("api_key")}
        me = next((w for w in workers if w["id"] == worker_id), None)
        if me and me.get("api_key"):
            for a in apis:
                if a["id"] == me["api_key"]:
                    return a
        for a in apis:
            if a["id"] not in occupied:
                if me is not None:
                    me["api_key"] = a["id"]
                    me["api_id"] = a["api_id"]
                    save_json(WORKER_FILE, workers)
                return a
    return None


def worker_public(w: dict) -> dict:
    ip = find_worker_proxy(w["id"])
    return {
        "id": w["id"],
        "phone": w.get("phone"),
        "remark": w.get("remark") or "",
        "status": w.get("status") or "offline",
        "status_text": w.get("status_text") or w.get("status") or "offline",
        "api_id": w.get("api_id"),
        "api_key": w.get("api_key"),
        "proxy": ip["display"] if ip else "",
    }


# --------------- telethon ---------------
async def _create_client(worker: dict):
    from telethon import TelegramClient

    api = pick_idle_api(worker["id"])
    if not api:
        raise RuntimeError("没有空闲 API，请先在 API 池添加（1 水军需要 1 个 API）")
    ip = find_worker_proxy(worker["id"])
    session_path = str(SESS / f"{worker['id']}")
    client = TelegramClient(
        session_path,
        int(api["api_id"]),
        api["api_hash"],
        proxy=telethon_proxy(ip) if ip else None,
        device_model="TG Forward Pro",
        system_version="1.0",
        app_version="1.0",
    )
    return client, api


async def _start_worker(worker_id: str) -> dict:
    workers = load_json(WORKER_FILE, [])
    w = next((x for x in workers if x["id"] == worker_id), None)
    if not w:
        return {"status": "error", "message": "水军不存在"}
    if worker_id in _clients:
        try:
            if _clients[worker_id].is_connected():
                _set_worker(worker_id, status="online", status_text="在线")
                return {"status": "ok", "message": "已在线"}
        except Exception:
            pass

    try:
        client, api = await _create_client(w)
    except Exception as e:
        _set_worker(worker_id, status="error", status_text=str(e)[:80])
        log(f"{w.get('phone')} 创建客户端失败: {e}", "error")
        return {"status": "error", "message": str(e)}

    await client.connect()
    if await client.is_user_authorized():
        _clients[worker_id] = client
        _set_worker(worker_id, status="online", status_text="在线", api_id=api["api_id"], api_key=api["id"])
        log(f"{w.get('phone')} 已用 API {api['api_id']} 登录")
        return {"status": "ok", "message": f"已登录，自动匹配 API {api['api_id']}"}

    try:
        await client.send_code_request(w["phone"])
    except Exception as e:
        await client.disconnect()
        _set_worker(worker_id, status="error", status_text=str(e)[:80])
        log(f"{w.get('phone')} 发送验证码失败: {e}", "error")
        return {"status": "error", "message": f"发送验证码失败: {e}"}

    _pending[worker_id] = {"client": client, "api": api}
    _set_worker(worker_id, status="wait_code", status_text="等待验证码", api_id=api["api_id"], api_key=api["id"])
    log(f"{w.get('phone')} 验证码已发送，已预占 API {api['api_id']}")
    return {"status": "ok", "need_code": True, "message": f"验证码已发送。已自动匹配 API {api['api_id']}"}


async def _verify_worker(worker_id: str, code: str, password: str) -> dict:
    from telethon.errors import SessionPasswordNeededError

    pend = _pending.get(worker_id)
    if not pend:
        return {"status": "error", "message": "请先点启动发送验证码"}
    client, api = pend["client"], pend["api"]
    workers = load_json(WORKER_FILE, [])
    w = next((x for x in workers if x["id"] == worker_id), None)
    phone = w["phone"] if w else ""
    try:
        try:
            await client.sign_in(phone, code)
        except SessionPasswordNeededError:
            pw = password or (w.get("twofa") if w else "") or ""
            if not pw:
                return {"status": "error", "message": "该号开启了两步验证，请填写密码"}
            await client.sign_in(password=pw)
        _clients[worker_id] = client
        _pending.pop(worker_id, None)
        _set_worker(worker_id, status="online", status_text="在线", api_id=api["api_id"], api_key=api["id"])
        log(f"{phone} 验证通过，API {api['api_id']} 已绑定")
        return {"status": "ok", "message": f"登录成功，已绑定 API {api['api_id']}"}
    except Exception as e:
        log(f"{phone} 验证失败: {e}", "error")
        return {"status": "error", "message": str(e)}


async def _stop_worker(worker_id: str) -> dict:
    await _detach_worker_groups(worker_id)
    client = _clients.pop(worker_id, None) or (_pending.pop(worker_id, {}) or {}).get("client")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass
    _set_worker(worker_id, status="offline", status_text="已停止")
    return {"status": "ok", "message": "已停止"}


def _set_worker(worker_id: str, **fields) -> None:
    with _lock:
        workers = load_json(WORKER_FILE, [])
        for w in workers:
            if w["id"] == worker_id:
                w.update(fields)
                break
        save_json(WORKER_FILE, workers)


async def _resolve(client, username: str):
    ent = normalize_entity(username)
    if re.fullmatch(r"-?\d+", ent):
        return int(ent)
    return await client.get_entity(ent)


async def _attach_group(group_id: str) -> dict:
    from telethon import events

    groups = load_json(GROUP_FILE, [])
    g = next((x for x in groups if x["id"] == group_id), None)
    if not g:
        return {"status": "error", "message": "转发组不存在"}
    if not g.get("targets"):
        return {"status": "error", "message": "请先给本组添加至少一个目标订阅号"}

    worker_id = g.get("worker_id")
    if worker_id and worker_id in _clients:
        pass
    else:
        # 自动匹配在线水军
        workers = load_json(WORKER_FILE, [])
        online = [w for w in workers if w.get("status") in ("online", "running") and w["id"] in _clients]
        if not online:
            return {"status": "error", "message": "没有在线水军，请先启动水军并完成验证"}
        worker_id = online[0]["id"]
        with _lock:
            groups = load_json(GROUP_FILE, [])
            for x in groups:
                if x["id"] == group_id:
                    x["worker_id"] = worker_id
            save_json(GROUP_FILE, groups)

    client = _clients.get(worker_id)
    if not client:
        return {"status": "error", "message": "指定水军未在线"}

    try:
        source = await _resolve(client, g["source"])
        targets = []
        for t in g["targets"]:
            targets.append(await _resolve(client, t["username"]))
    except Exception as e:
        log(f"解析订阅失败: {e}", "error")
        return {"status": "error", "message": f"解析源/目标失败: {e}"}

    await _detach_group(group_id)

    async def handler(event):
        if not getattr(event, "message", None):
            return
        try:
            for dest in targets:
                try:
                    await client.forward_messages(dest, event.message)
                    bump_forwarded(1)
                    with _lock:
                        gs = load_json(GROUP_FILE, [])
                        for x in gs:
                            if x["id"] == group_id:
                                x["forwarded"] = int(x.get("forwarded") or 0) + 1
                        save_json(GROUP_FILE, gs)
                except Exception as fe:
                    log(f"组 {group_id} 转发失败: {fe}", "error")
        except Exception as e:
            log(f"组 {group_id} 处理消息失败: {e}", "error")

    h = client.add_event_handler(handler, events.NewMessage(chats=source))
    _handlers[group_id] = {"handler": handler, "client_id": worker_id, "raw": h}
    with _lock:
        groups = load_json(GROUP_FILE, [])
        for x in groups:
            if x["id"] == group_id:
                x["running"] = True
                x["worker_id"] = worker_id
        save_json(GROUP_FILE, groups)
    log(f"转发组 {group_id} 已启动：{g['source']} → {len(g['targets'])} 个目标")
    return {"status": "ok", "message": f"本组已启动，监听 {g['source']}"}


async def _detach_group(group_id: str) -> None:
    info = _handlers.pop(group_id, None)
    if not info:
        return
    client = _clients.get(info.get("client_id"))
    if client:
        try:
            client.remove_event_handler(info["handler"])
        except Exception:
            pass
    with _lock:
        groups = load_json(GROUP_FILE, [])
        for x in groups:
            if x["id"] == group_id:
                x["running"] = False
        save_json(GROUP_FILE, groups)


async def _detach_worker_groups(worker_id: str) -> None:
    groups = load_json(GROUP_FILE, [])
    for g in groups:
        if g.get("worker_id") == worker_id and g.get("running"):
            await _detach_group(g["id"])


async def _delete_worker(worker_id: str) -> None:
    await _stop_worker(worker_id)
    with _lock:
        workers = [w for w in load_json(WORKER_FILE, []) if w["id"] != worker_id]
        save_json(WORKER_FILE, workers)
        ips = load_json(IP_FILE, [])
        for ip in ips:
            ip["worker_ids"] = [x for x in (ip.get("worker_ids") or []) if x != worker_id]
        save_json(IP_FILE, ips)
    for ext in (".session", ".session-journal"):
        p = SESS / f"{worker_id}{ext}"
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass


# --------------- auth ---------------
@app.before_request
def _auth():
    path = request.path
    open_paths = {"/api/login", "/api/auth/status", "/login.html"}
    if path.startswith("/static/") or path in open_paths:
        return None
    if path == "/" and not session.get("user"):
        return send_from_directory(STATIC, "login.html")
    if path.startswith("/api/") and not session.get("user"):
        return jsonify({"status": "error", "message": "未登录"}), 401
    return None


@app.route("/")
def index():
    return send_from_directory(STATIC, "index.html")


@app.route("/login.html")
def login_page():
    return send_from_directory(STATIC, "login.html")


@app.post("/api/login")
def api_login():
    body = request.get_json(silent=True) or {}
    if body.get("username") == ADMIN_USERNAME and body.get("password") == ADMIN_PASSWORD:
        session["user"] = body["username"]
        return jsonify({"status": "ok"})
    return jsonify({"status": "error", "message": "用户名或密码错误"})


@app.post("/api/logout")
def api_logout():
    session.clear()
    return jsonify({"status": "ok"})


@app.get("/api/auth/status")
def api_auth_status():
    return jsonify({"logged_in": bool(session.get("user")), "username": session.get("user")})


# --------------- overview ---------------
@app.get("/api/overview")
def api_overview():
    workers = load_json(WORKER_FILE, [])
    apis = load_json(API_FILE, [])
    ips = load_json(IP_FILE, [])
    groups = load_json(GROUP_FILE, [])
    st = load_json(STATS_FILE, {"forwarded": 0})
    occupied = {w.get("api_key"): w.get("phone") for w in workers if w.get("api_key")}
    api_view = []
    for a in apis:
        api_view.append({
            "id": a["id"],
            "api_id": a["api_id"],
            "api_hash": a["api_hash"],
            "worker_phone": occupied.get(a["id"], ""),
        })
    return jsonify({
        "workers": [worker_public(w) for w in workers],
        "apis": api_view,
        "ips": ips,
        "groups": groups,
        "stats": {
            "workers": len(workers),
            "online_workers": sum(1 for w in workers if w.get("status") in ("online", "running")),
            "apis": len(apis),
            "ips": len(ips),
            "groups": len(groups),
            "running_groups": sum(1 for g in groups if g.get("running")),
            "forwarded": st.get("forwarded", 0),
        },
    })


@app.get("/api/logs")
def api_logs():
    return jsonify({"logs": load_json(LOG_FILE, [])[-200:]})


# --------------- API pool ---------------
@app.post("/api/pools/api")
def api_add_apis():
    body = request.get_json(silent=True) or {}
    items = parse_api_lines(body.get("text") or "")
    if not items:
        return jsonify({"status": "error", "message": "没有可识别的 api_id,api_hash"})
    with _lock:
        pool = load_json(API_FILE, [])
        exist = {(int(x["api_id"]), x["api_hash"]) for x in pool}
        added = 0
        for it in items:
            key = (int(it["api_id"]), it["api_hash"])
            if key in exist:
                continue
            pool.append({"id": new_id("api_"), "api_id": it["api_id"], "api_hash": it["api_hash"]})
            exist.add(key)
            added += 1
        save_json(API_FILE, pool)
    log(f"API 池一键添加 {added} 条")
    return jsonify({"status": "ok", "added": added, "message": f"已新增 {added} 条 API"})


@app.delete("/api/pools/api/<api_id>")
def api_del_api(api_id: str):
    with _lock:
        pool = load_json(API_FILE, [])
        target = next((x for x in pool if x["id"] == api_id), None)
        if not target:
            return jsonify({"status": "error", "message": "API 不存在"})
        workers = load_json(WORKER_FILE, [])
        using = [w for w in workers if w.get("api_key") == api_id]
        if using:
            return jsonify({"status": "error", "message": f"该 API 正被 {using[0].get('phone')} 占用，请先停止该水军"})
        save_json(API_FILE, [x for x in pool if x["id"] != api_id])
    log(f"删除 API {target.get('api_id')}")
    return jsonify({"status": "ok", "message": "已删除"})


# --------------- IP pool ---------------
@app.post("/api/pools/ip")
def api_add_ips():
    body = request.get_json(silent=True) or {}
    text = body.get("text") or ""
    added = 0
    with _lock:
        pool = load_json(IP_FILE, [])
        exist = {x.get("raw") for x in pool}
        for line in text.splitlines():
            item = parse_proxy_line(line)
            if not item:
                continue
            if item["raw"] in exist:
                continue
            pool.append(item)
            exist.add(item["raw"])
            added += 1
        save_json(IP_FILE, pool)
    log(f"IP 池新增 {added} 条")
    return jsonify({"status": "ok", "added": added, "message": f"已新增 {added} 条代理"})


@app.post("/api/pools/ip/<ip_id>/bind")
def api_bind_ip(ip_id: str):
    body = request.get_json(silent=True) or {}
    worker_ids = body.get("worker_ids") or []
    with _lock:
        pool = load_json(IP_FILE, [])
        workers = load_json(WORKER_FILE, [])
        known = {w["id"] for w in workers}
        worker_ids = [x for x in worker_ids if x in known]
        found = False
        for ip in pool:
            if ip["id"] == ip_id:
                ip["worker_ids"] = worker_ids
                found = True
            else:
                # 一个水军只绑一条 IP
                ip["worker_ids"] = [x for x in (ip.get("worker_ids") or []) if x not in worker_ids]
        if not found:
            return jsonify({"status": "error", "message": "代理不存在"})
        save_json(IP_FILE, pool)
    log(f"代理 {ip_id} 绑定水军 {len(worker_ids)} 个")
    return jsonify({"status": "ok", "message": f"已绑定 {len(worker_ids)} 个水军号"})


@app.delete("/api/pools/ip/<ip_id>")
def api_del_ip(ip_id: str):
    with _lock:
        pool = load_json(IP_FILE, [])
        save_json(IP_FILE, [x for x in pool if x["id"] != ip_id])
    return jsonify({"status": "ok", "message": "已删除"})


# --------------- workers ---------------
@app.post("/api/workers")
def api_add_worker():
    body = request.get_json(silent=True) or {}
    phone = (body.get("phone") or "").strip()
    if not phone:
        return jsonify({"status": "error", "message": "手机号必填"})
    with _lock:
        workers = load_json(WORKER_FILE, [])
        if any(w.get("phone") == phone for w in workers):
            return jsonify({"status": "error", "message": "该手机号已存在"})
        w = {
            "id": new_id("w_"),
            "phone": phone,
            "twofa": body.get("twofa") or "",
            "remark": (body.get("remark") or "").strip(),
            "status": "offline",
            "status_text": "未启动",
            "created_at": now_str(),
        }
        workers.append(w)
        save_json(WORKER_FILE, workers)
    log(f"添加水军 {phone}")
    return jsonify({"status": "ok", "id": w["id"]})


@app.post("/api/workers/<wid>/remark")
def api_worker_remark(wid: str):
    body = request.get_json(silent=True) or {}
    _set_worker(wid, remark=(body.get("remark") or "").strip())
    return jsonify({"status": "ok"})


@app.post("/api/workers/<wid>/start")
def api_worker_start(wid: str):
    try:
        return jsonify(run_async(_start_worker(wid), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@app.post("/api/workers/<wid>/verify")
def api_worker_verify(wid: str):
    body = request.get_json(silent=True) or {}
    try:
        return jsonify(run_async(_verify_worker(wid, body.get("code") or "", body.get("password") or ""), timeout=90))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@app.post("/api/workers/<wid>/stop")
def api_worker_stop(wid: str):
    try:
        return jsonify(run_async(_stop_worker(wid), timeout=30))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@app.delete("/api/workers/<wid>")
def api_worker_del(wid: str):
    try:
        run_async(_delete_worker(wid), timeout=30)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


# --------------- forward groups ---------------
@app.post("/api/groups")
def api_add_group():
    body = request.get_json(silent=True) or {}
    source = normalize_entity(body.get("source") or "")
    if not source:
        return jsonify({"status": "error", "message": "源订阅号必填"})
    g = {
        "id": new_id("g_"),
        "source": source,
        "source_remark": (body.get("source_remark") or "").strip(),
        "targets": [],
        "worker_id": "",
        "running": False,
        "forwarded": 0,
        "created_at": now_str(),
    }
    with _lock:
        groups = load_json(GROUP_FILE, [])
        groups.insert(0, g)
        save_json(GROUP_FILE, groups)
    log(f"新建转发组 {source}")
    return jsonify({"status": "ok", "id": g["id"]})


@app.post("/api/groups/<gid>/source-remark")
def api_group_src_remark(gid: str):
    body = request.get_json(silent=True) or {}
    with _lock:
        groups = load_json(GROUP_FILE, [])
        for g in groups:
            if g["id"] == gid:
                g["source_remark"] = (body.get("source_remark") or "").strip()
        save_json(GROUP_FILE, groups)
    return jsonify({"status": "ok"})


@app.post("/api/groups/<gid>/worker")
def api_group_worker(gid: str):
    body = request.get_json(silent=True) or {}
    with _lock:
        groups = load_json(GROUP_FILE, [])
        for g in groups:
            if g["id"] == gid:
                g["worker_id"] = body.get("worker_id") or ""
        save_json(GROUP_FILE, groups)
    return jsonify({"status": "ok"})


@app.post("/api/groups/<gid>/targets")
def api_add_target(gid: str):
    body = request.get_json(silent=True) or {}
    username = normalize_entity(body.get("username") or "")
    if not username:
        return jsonify({"status": "error", "message": "目标订阅号必填"})
    with _lock:
        groups = load_json(GROUP_FILE, [])
        g = next((x for x in groups if x["id"] == gid), None)
        if not g:
            return jsonify({"status": "error", "message": "组不存在"})
        if any(normalize_entity(t.get("username")) == username for t in g.get("targets") or []):
            return jsonify({"status": "error", "message": "该目标已在本组"})
        g.setdefault("targets", []).append({
            "id": new_id("t_"),
            "username": username,
            "remark": (body.get("remark") or "").strip(),
        })
        save_json(GROUP_FILE, groups)
    return jsonify({"status": "ok"})


@app.delete("/api/groups/<gid>/targets/<tid>")
def api_del_target(gid: str, tid: str):
    with _lock:
        groups = load_json(GROUP_FILE, [])
        for g in groups:
            if g["id"] == gid:
                g["targets"] = [t for t in g.get("targets") or [] if t["id"] != tid]
        save_json(GROUP_FILE, groups)
    return jsonify({"status": "ok"})


@app.post("/api/groups/<gid>/start")
def api_group_start(gid: str):
    try:
        return jsonify(run_async(_attach_group(gid), timeout=60))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@app.post("/api/groups/<gid>/stop")
def api_group_stop(gid: str):
    try:
        run_async(_detach_group(gid), timeout=20)
        return jsonify({"status": "ok", "message": "本组已停止"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@app.delete("/api/groups/<gid>")
def api_del_group(gid: str):
    try:
        run_async(_detach_group(gid), timeout=20)
    except Exception:
        pass
    with _lock:
        save_json(GROUP_FILE, [g for g in load_json(GROUP_FILE, []) if g["id"] != gid])
    return jsonify({"status": "ok"})


def main():
    start_loop()
    host = os.environ.get("TG_HOST", "0.0.0.0")
    port = int(os.environ.get("TG_PORT", "8088"))
    log(f"TG 订阅转发 Pro 启动 {host}:{port}")
    app.run(host=host, port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()

@app.post("/api/bots_old")
def api_bots_add_fix():
    body = request.get_json(silent=True) or {}
    token = (body.get("token") or "").strip()
    remark = (body.get("remark") or "").strip()
    if not token or ":" not in token:
        return jsonify({"status":"error","message":"请填写 Bot Token"})
    path = DATA / "bots.json"
    with _lock:
        bots = load_json(path, [])
        if any(b.get("token")==token for b in bots):
            return jsonify({"status":"error","message":"该 Bot 已存在"})
        bots.append({"id": new_id("bot_"), "token": token, "remark": remark or "发布Bot", "created_at": now_str()})
        save_json(path, bots)
    return jsonify({"status":"ok"})

@app.get("/api/bots")
def api_bots_list_fix():
    return jsonify({"bots": load_json(DATA / "bots.json", [])})

@app.post("/api/bots_old")
def api_bots_add_fix2():
    body = request.get_json(silent=True) or {}
    token = (body.get("token") or "").strip()
    remark = (body.get("remark") or "").strip()
    if not token or ":" not in token:
        return jsonify({"status":"error","message":"请填写 Bot Token"})
    path = DATA / "bots.json"
    with _lock:
        bots = load_json(path, [])
        if any(b.get("token")==token for b in bots):
            return jsonify({"status":"error","message":"该 Bot 已存在"})
        bots.append({"id": new_id("bot_"), "token": token, "remark": remark or "发布Bot", "created_at": now_str()})
        save_json(path, bots)
    return jsonify({"status":"ok"})

@app.get("/api/bots")
def api_bots_list_fix_2():
    return jsonify({"bots": load_json(DATA / "bots.json", [])})

