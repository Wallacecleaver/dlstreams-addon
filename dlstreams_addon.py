#!/usr/bin/env python3
"""dlstreams -> Stremio : mini-addon + proxy autonome avec dashboard complet.
Dashboard avec session persistante, gestion des sources, et navigation SPA.
"""
from __future__ import annotations
import base64
import json
import os
import re
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8781"))

# Mot de passe dashboard : si DASHBOARD_PASSWORD n'est pas fourni en variable
# d'environnement, on en genere un aleatoire au demarrage plutot que d'utiliser
# un defaut fixe ("admin123") -- un mot de passe connu de tous rend la
# protection inutile. Il change a chaque redemarrage ; definis DASHBOARD_PASSWORD
# sur Render si tu veux un mot de passe stable entre deploiements.
_PASSWORD_GENERATED = "DASHBOARD_PASSWORD" not in os.environ
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD") or secrets.token_urlsafe(9)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
SITE = "https://dlstreams.st"
_CH_TTL = 1800
_START_TIME = time.time()
_request_count = 0
_error_count = 0
_stats_lock = threading.Lock()

# --- sessions dashboard : jeton opaque valide cote serveur, stocke en memoire
# (pas de JWT/dependance -- juste un dict token -> heure d'emission). Remplace
# l'ancien systeme qui ne verifiait le mot de passe qu'une fois sans jamais
# proteger les endpoints /api/* ensuite. ---
_sessions: dict[str, float] = {}
_SESSION_TTL = 24 * 3600
_login_attempts: dict[str, list[float]] = {}   # ip -> heures des echecs recents
_LOGIN_MAX_ATTEMPTS = 6
_LOGIN_WINDOW = 300  # 5 minutes

_POPULAR_CHANNELS = [
    {"id": "121", "name": "Canal+ France", "lang": "fr"},
    {"id": "122", "name": "Canal+ Sport", "lang": "fr"},
    {"id": "123", "name": "Canal+ Cinéma", "lang": "fr"},
    {"id": "124", "name": "Canal+ Séries", "lang": "fr"},
    {"id": "125", "name": "Canal+ Family", "lang": "fr"},
    {"id": "201", "name": "beIN Sports 1", "lang": "fr"},
    {"id": "202", "name": "beIN Sports 2", "lang": "fr"},
    {"id": "203", "name": "beIN Sports 3", "lang": "fr"},
    {"id": "211", "name": "RMC Sport 1", "lang": "fr"},
    {"id": "212", "name": "RMC Sport 2", "lang": "fr"},
    {"id": "213", "name": "RMC Sport 3", "lang": "fr"},
    {"id": "214", "name": "RMC Sport 4", "lang": "fr"},
    {"id": "301", "name": "Eurosport 1", "lang": "fr"},
    {"id": "302", "name": "Eurosport 2", "lang": "fr"},
    {"id": "401", "name": "TF1", "lang": "fr"},
    {"id": "402", "name": "France 2", "lang": "fr"},
    {"id": "403", "name": "France 3", "lang": "fr"},
    {"id": "404", "name": "France 4", "lang": "fr"},
    {"id": "405", "name": "France 5", "lang": "fr"},
    {"id": "406", "name": "M6", "lang": "fr"},
    {"id": "407", "name": "Arte", "lang": "fr"},
    {"id": "408", "name": "C8", "lang": "fr"},
    {"id": "409", "name": "W9", "lang": "fr"},
    {"id": "410", "name": "TMC", "lang": "fr"},
    {"id": "411", "name": "TFX", "lang": "fr"},
    {"id": "412", "name": "NRJ 12", "lang": "fr"},
    {"id": "413", "name": "LCP", "lang": "fr"},
    {"id": "414", "name": "France Info", "lang": "fr"},
    {"id": "415", "name": "BFM TV", "lang": "fr"},
    {"id": "416", "name": "CNews", "lang": "fr"},
    {"id": "417", "name": "CStar", "lang": "fr"},
    {"id": "418", "name": "Gulli", "lang": "fr"},
    {"id": "419", "name": "TF1 Séries Films", "lang": "fr"},
    {"id": "420", "name": "L'Équipe", "lang": "fr"},
    {"id": "421", "name": "6ter", "lang": "fr"},
    {"id": "422", "name": "RMC Story", "lang": "fr"},
    {"id": "423", "name": "RMC Découverte", "lang": "fr"},
    {"id": "424", "name": "Chérie 25", "lang": "fr"},
]

def _detect_lang(name: str) -> str:
    n = name.lower()
    if any(x in n for x in ["france", "français", "french", " fr ", "tf1", "france 2", "france 3", "m6", "canal+", "rmc", "l'équipe", "arte", "bein sports"]):
        return "fr"
    if any(x in n for x in ["uk", "english", "usa", "espn", "fox", "cnn", "nbc", "sky sports"]):
        return "en"
    if any(x in n for x in ["españa", "spanish", "spain", " es ", "movistar"]):
        return "es"
    if any(x in n for x in ["deutsch", "german", "germany", " de ", "ard", "zdf"]):
        return "de"
    if any(x in n for x in ["italia", "italian", "italy", " it ", "rai"]):
        return "it"
    if any(x in n for x in ["arabic", "arabe", "mbc", "al jazeera"]):
        return "ar"
    if any(x in n for x in ["portugal", "portuguese", " pt ", "sport tv"]):
        return "pt"
    return "other"

def _get(url: str, referer: str = SITE + "/", extra: dict | None = None, timeout: int = 20) -> bytes:
    headers = {"User-Agent": UA, "Referer": referer}
    if extra:
        headers.update(extra)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

class _LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.items: list[tuple[str, str]] = []
        self._cur_id: str | None = None
        self._buf: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "") or ""
            if "watch.php?id=" in href:
                idv = href.split("watch.php?id=", 1)[1].split("&", 1)[0].strip()
                if idv.isdigit():
                    self._cur_id = idv
                    self._buf = []

    def handle_data(self, data):
        if self._cur_id is not None:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._cur_id is not None:
            name = " ".join("".join(self._buf).split()).strip()
            if name:
                self.items.append((self._cur_id, name))
            self._cur_id = None

_ch_cache: dict = {"at": 0.0, "list": []}
_manual_channels: dict[str, dict] = {}
_activity_log: list[dict] = []

def _log_activity(action: str, details: str = ""):
    _activity_log.append({
        "time": time.strftime("%H:%M:%S"),
        "action": action,
        "details": details
    })
    if len(_activity_log) > 100:
        _activity_log.pop(0)

def channels(lang_filter: str | None = None) -> list[dict]:
    # BUG corrige : avant, le cache n'etait reutilise que si lang_filter is None.
    # Le dashboard appelle toujours /api/channels?lang=fr, donc CHAQUE appel
    # re-scrapait dlstreams.st en direct (le TTL de 30 min etait ignore en
    # permanence). Desormais on ne re-scrape que si le cache est vide/perime,
    # et le filtre langue s'applique en memoire sur la liste deja en cache.
    now = time.time()
    if not _ch_cache["list"] or now - _ch_cache["at"] >= _CH_TTL:
        seen: dict[str, dict] = {ch["id"]: ch for ch in _POPULAR_CHANNELS}
        seen.update(_manual_channels)

        try:
            html = _get(SITE + "/").decode("utf-8", "replace")
            p = _LinkParser()
            p.feed(html)
            for idv, name in p.items:
                if idv not in seen:
                    seen[idv] = {"id": idv, "name": name, "lang": _detect_lang(name)}
        except Exception:
            pass

        _ch_cache.update(at=now, list=list(seen.values()))

    out = _ch_cache["list"]
    if lang_filter and lang_filter != "all":
        out = [c for c in out if c.get("lang") == lang_filter]
    return out

def scrape_channels_from_url(url: str) -> tuple[list[dict], str]:
    try:
        html = _get(url).decode("utf-8", "replace")
        p = _LinkParser()
        p.feed(html)
        
        if not p.items:
            return [], "Aucune chaîne trouvée sur cette page"
        
        added = []
        existing_count = 0
        current_channels = {ch["id"]: ch for ch in _POPULAR_CHANNELS}
        current_channels.update(_manual_channels)
        
        for idv, name in p.items:
            if idv not in current_channels:
                ch = {"id": idv, "name": name, "lang": _detect_lang(name), "added_at": time.strftime("%Y-%m-%d %H:%M")}
                _manual_channels[idv] = ch
                added.append(ch)
            else:
                existing_count += 1
        
        message = f"✅ {len(added)} chaîne(s) ajoutée(s)"
        if existing_count > 0:
            message += f" ({existing_count} déjà existantes)"
        
        if added:
            _log_activity("Ajout de sources", f"{len(added)} chaînes depuis {url}")
        
        return added, message
    except Exception as e:
        return [], f"❌ Erreur: {type(e).__name__}: {e}"

def remove_manual_channel(channel_id: str) -> bool:
    if channel_id in _manual_channels:
        removed = _manual_channels.pop(channel_id)
        _log_activity("Suppression", f"Chaîne {removed['name']} (ID: {channel_id})")
        _ch_cache["list"] = []
        _ch_cache["at"] = 0
        return True
    return False

_PLAYER_PATHS = ("stream", "watch", "player", "plus", "hub", "cast", "casting")

def _txt(url: str, referer: str = SITE + "/") -> str:
    return _get(url, referer=referer).decode("utf-8", "replace")

def players(cid: str) -> list[tuple[str, str]]:
    try:
        w = _txt(f"{SITE}/watch.php?id={cid}")
    except Exception:
        w = ""
    pairs = re.findall(r'data-url="([^"]+)"[^>]*title="([^"]*)"', w)
    out = [(title.strip() or f"Player {i + 1}", url)
           for i, (url, title) in enumerate(pairs) if url.startswith("http")]
    return out or [(f"Player {i + 1}", f"{SITE}/{p}/stream-{cid}.php")
                   for i, p in enumerate(_PLAYER_PATHS)]

def _first_iframe(html: str) -> str | None:
    return next((m for m in re.findall(r'<iframe[^>]+src="([^"]+)"', html) if m.startswith("http")), None)

def _find_m3u8(html: str) -> str | None:
    seg = html
    while "atob('" in seg:
        seg = seg.split("atob('", 1)[1]
        b = seg[:seg.find("'")]
        try:
            dec = base64.b64decode(b + "=" * (-len(b) % 4)).decode("utf-8", "replace")
        except Exception:
            dec = ""
        if ".m3u8" in dec:
            return dec
    m = re.search(r'https?://\S+?\.m3u8[^"\'\s\\]*', html.replace("\\/", "/"))
    return m.group(0) if m else None

def resolve_player(stream_url: str) -> tuple[str, str]:
    emb = _first_iframe(_txt(stream_url, referer=SITE + "/watch.php"))
    if not emb:
        raise ValueError("pas d'iframe")
    host = urllib.parse.urlsplit(emb).scheme + "://" + urllib.parse.urlsplit(emb).netloc
    html = _txt(emb)
    mu = _find_m3u8(html)
    if not mu:
        emb2 = _first_iframe(html)
        if emb2:
            host = urllib.parse.urlsplit(emb2).scheme + "://" + urllib.parse.urlsplit(emb2).netloc
            mu = _find_m3u8(_txt(emb2, referer=emb))
    if not mu:
        raise ValueError("m3u8 introuvable (hors-antenne ?)")
    return mu, host

def resolve(cid: str) -> tuple[str, str]:
    for _label, url in players(cid):
        try:
            return resolve_player(url)
        except Exception:
            continue
    raise ValueError("aucun player ne resout")

def working_players(cid: str) -> list[tuple[int, str]]:
    pls = players(cid)
    def _chk(item: tuple[int, tuple[str, str]]) -> tuple[int, str] | None:
        i, (label, url) = item
        try:
            resolve_player(url)
            return (i, label)
        except Exception:
            return None
    with ThreadPoolExecutor(max_workers=8) as ex:
        return [x for x in ex.map(_chk, enumerate(pls)) if x]

_VAVOO_MIRRORS = (("https://oha.cx", "mediaurl", False),
                  ("http://178.239.115.119", "mediaurl", False),
                  ("https://vavoo.net", "mediahubmx", True),
                  ("https://kool.ws", "mediahubmx", True))
_VAVOO_PINGS = ("https://www.lokke.app/api/app/ping", "https://www.vavoo.tv/api/app/ping",
                "https://www.vypn.net/api/app/ping")
_VAVOO_UA = "MediaHubMX/2"
_vavoo_cache: dict = {"at": 0.0, "list": []}
_vavoo_sig: dict = {"v": "", "at": 0.0}
_vavoo_base: dict = {"url": ""}

def _post_json(url: str, body: dict, headers: dict, timeout: int = 20):
    data = json.dumps(body).encode()
    h = {"content-type": "application/json; charset=utf-8", **headers}
    req = urllib.request.Request(url, data=data, headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))

def _vavoo_ping_body() -> dict:
    now = int(time.time() * 1000)
    return {"token": "", "reason": "app-focus", "locale": "fr", "theme": "dark",
            "metadata": {"device": {"type": "phone", "uniqueId": "vypn-" + uuid.uuid4().hex[:16]},
                         "os": {"name": "android", "version": "13", "abis": ["arm64-v8a"], "host": "android"},
                         "app": {"platform": "android"},
                         "version": {"package": "net.vypn.app", "binary": "1.4.1", "js": "1.4.1"}},
            "appFocusTime": 0, "playerActive": False, "playDuration": 0, "devMode": False,
            "hasAddon": True, "castConnected": False, "package": "net.vypn.app", "version": "1.4.1",
            "process": {"firstAppStart": now, "lastAppStart": now},
            "firstAppStart": now, "lastAppStart": now, "ipLocation": None, "adblockEnabled": True,
            "migrationApplied": False, "migrationTargetInstalled": False,
            "proxy": {"supported": ["ss"], "engine": "Mu", "ssVersion": "2022", "enabled": False,
                      "autoServer": True, "id": ""}}

def _vavoo_signature(force: bool = False) -> str:
    if not force and _vavoo_sig["v"] and time.time() - _vavoo_sig["at"] < 3600:
        return _vavoo_sig["v"]
    for host in _VAVOO_PINGS:
        try:
            d = _post_json(host, _vavoo_ping_body(), {"user-agent": _VAVOO_UA}, timeout=15)
        except Exception:
            continue
        v = (d or {}).get("addonSig")
        if v:
            _vavoo_sig.update(v=v, at=time.time())
            return v
    return ""

def _vavoo_post(action: str, body: dict):
    order = sorted(_VAVOO_MIRRORS, key=lambda m: m[0] != _vavoo_base["url"])
    for base, prefix, need in order:
        for attempt in (0, 1):
            h = {"user-agent": _VAVOO_UA, "Accept-Language": "fr"}
            if need:
                h["mediahubmx-signature"] = _vavoo_signature()
            try:
                d = _post_json(f"{base}/{prefix}-{action}.json", body, h)
            except urllib.error.HTTPError as e:
                if need and e.code in (401, 403) and attempt == 0:
                    _vavoo_signature(force=True)
                    continue
                break
            except Exception:
                break
            _vavoo_base["url"] = base
            return d
    return None

def vavoo_channels(country: str = "France") -> list[dict]:
    if _vavoo_cache["list"] and time.time() - _vavoo_cache["at"] < 6 * 3600:
        return _vavoo_cache["list"]
    items, cursor, pages = [], 0, 0
    while cursor is not None and pages < 40:
        d = _vavoo_post("catalog", {"language": "fr", "region": "FR", "catalogId": "iptv", "id": "",
                                    "adult": False, "search": "", "sort": "name",
                                    "filter": {"group": country}, "cursor": cursor, "clientVersion": "3.1.0"})
        if not d:
            break
        batch = d.get("items") or []
        if not batch:
            break
        items += [{"id": x.get("url"), "name": x.get("name") or "", "logo": x.get("logo") or "", "lang": "fr"}
                  for x in batch if x.get("url")]
        cursor, pages = d.get("nextCursor"), pages + 1
    if items:
        _vavoo_cache.update(at=time.time(), list=items)
    return _vavoo_cache["list"]

def vavoo_resolve(vurl: str) -> str:
    if not vurl:
        return ""
    d = _vavoo_post("resolve", {"language": "fr", "region": "FR", "url": str(vurl), "clientVersion": "3.0.2"})
    if isinstance(d, list) and d:
        return d[0].get("url") or d[0].get("streamUrl") or ""
    return ""

def _b64u(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")

def _unb64u(s: str) -> str:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()

def _proxy_get(url: str, hdr: dict, timeout: int = 25) -> bytes:
    headers = {"User-Agent": UA}
    headers.update(hdr or {})
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def _rewrite_playlist(text: str, playlist_url: str, hdr_enc: str, self_base: str) -> str:
    base = playlist_url.rsplit("/", 1)[0] + "/"
    out = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            out.append(line)
            continue
        absu = s if s.startswith("http") else urllib.parse.urljoin(base, s)
        route = "px" if ".m3u8" in absu.split("?", 1)[0] else "sx"
        out.append(f"{self_base}/{route}?u={_b64u(absu)}&h={hdr_enc}")
    return "\n".join(out)

def _stats() -> dict:
    all_ch = channels()
    lang_counts = {}
    for ch in all_ch:
        lang = ch.get("lang", "other")
        lang_counts[lang] = lang_counts.get(lang, 0) + 1
    
    return {
        "status": "ok",
        "uptime": int(time.time() - _START_TIME),
        "version": "1.5.0",
        "port": PORT,
        "requests": _request_count,
        "errors": _error_count,
        "manual_channels": len(_manual_channels),
        "dlstreams": {
            "count": len(_ch_cache.get("list") or []),
            "age_seconds": int(time.time() - _ch_cache.get("at", 0)) if _ch_cache.get("at") else None,
        },
        "vavoo": {
            "count": len(_vavoo_cache.get("list") or []),
            "age_seconds": int(time.time() - _vavoo_cache.get("at", 0)) if _vavoo_cache.get("at") else None,
        },
        "lang_counts": lang_counts,
    }

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, code: int, body: bytes, ctype: str, cache: bool = False):
        global _request_count, _error_count
        with _stats_lock:
            _request_count += 1
            if code >= 400:
                _error_count += 1
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        if cache:
            self.send_header("Cache-Control", "public, max-age=60")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _self_base(self) -> str:
        host = self.headers.get("Host") or f"127.0.0.1:{PORT}"
        proto = self.headers.get("X-Forwarded-Proto", "http").split(",")[0].strip()
        return f"{proto}://{host}"

    def _client_ip(self) -> str:
        fwd = self.headers.get("X-Forwarded-For", "")
        return fwd.split(",")[0].strip() if fwd else self.client_address[0]

    def _cookie(self, name: str) -> str:
        raw = self.headers.get("Cookie", "")
        for part in raw.split(";"):
            k, _, v = part.strip().partition("=")
            if k == name:
                return v
        return ""

    def _authed(self) -> bool:
        tok = self._cookie("dl_session")
        if not tok:
            return False
        with _stats_lock:
            issued = _sessions.get(tok)
        return bool(issued) and (time.time() - issued) < _SESSION_TTL

    def _require_auth(self) -> bool:
        """Renvoie True si authentifie ; sinon envoie 401 JSON et renvoie False."""
        if self._authed():
            return True
        self._send(401, json.dumps({"success": False, "message": "authentification requise"}).encode(),
                   "application/json")
        return False

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length > 0 else b''

        u = urllib.parse.urlsplit(self.path)
        path = u.path

        if path == "/api/auth":
            ip = self._client_ip()
            now = time.time()
            with _stats_lock:
                attempts = [t for t in _login_attempts.get(ip, []) if now - t < _LOGIN_WINDOW]
                _login_attempts[ip] = attempts
            if len(attempts) >= _LOGIN_MAX_ATTEMPTS:
                return self._send(429, json.dumps({"success": False,
                                  "message": "trop de tentatives, reessaie dans quelques minutes"}).encode(),
                                  "application/json")
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {}
            password = str(data.get("password", ""))
            if secrets.compare_digest(password, DASHBOARD_PASSWORD):
                token = secrets.token_urlsafe(24)
                with _stats_lock:
                    _sessions[token] = time.time()
                _log_activity("Connexion réussie")
                resp = json.dumps({"success": True}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                secure = "; Secure" if self.headers.get("X-Forwarded-Proto", "http").startswith("https") else ""
                self.send_header("Set-Cookie",
                                  f"dl_session={token}; HttpOnly; Path=/; Max-Age={_SESSION_TTL}; SameSite=Lax{secure}")
                self.end_headers()
                self.wfile.write(resp)
                return
            with _stats_lock:
                _login_attempts.setdefault(ip, []).append(now)
            _log_activity("Tentative de connexion échouée")
            return self._send(401, json.dumps({"success": False, "message": "mot de passe incorrect"}).encode(),
                              "application/json")

        if path == "/api/logout":
            tok = self._cookie("dl_session")
            with _stats_lock:
                _sessions.pop(tok, None)
            resp = json.dumps({"success": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp)))
            self.send_header("Set-Cookie", "dl_session=; HttpOnly; Path=/; Max-Age=0")
            self.end_headers()
            self.wfile.write(resp)
            return

        if path == "/api/remove-channel":
            if not self._require_auth():
                return
            try:
                data = json.loads(body) if body else {}
                channel_id = data.get("id")
                if not channel_id:
                    return self._send(400, json.dumps({"success": False, "message": "ID manquant"}).encode(), "application/json")

                if remove_manual_channel(channel_id):
                    return self._send(200, json.dumps({"success": True, "message": "Chaîne supprimée"}).encode(), "application/json")
                else:
                    return self._send(404, json.dumps({"success": False, "message": "Chaîne non trouvée"}).encode(), "application/json")
            except Exception as e:
                return self._send(500, json.dumps({"success": False, "message": str(e)}).encode(), "application/json")

        return self._send(404, b"not found", "text/plain")

    def do_GET(self):
        u = urllib.parse.urlsplit(self.path)
        path = u.path
        qs = urllib.parse.parse_qs(u.query)
        try:
            if path == "/dashboard" or path == "/dashboard.html":
                return self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8", True)

            if path == "/configure" or path == "/configure.html":
                return self._send(200, CONFIGURE_HTML.encode("utf-8"), "text/html; charset=utf-8", True)

            if path == "/api/stats":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(_stats()).encode(), "application/json")

            if path == "/api/channels":
                if not self._require_auth():
                    return
                lang = qs.get("lang", [None])[0]
                return self._send(200, json.dumps(channels(lang_filter=lang)).encode(), "application/json", True)

            if path == "/api/vavoo-channels":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(vavoo_channels()).encode(), "application/json", True)

            if path == "/api/manual-channels":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(list(_manual_channels.values())).encode(), "application/json")

            if path == "/api/activity":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(list(reversed(_activity_log))).encode(), "application/json")

            if path == "/api/add-source":
                if not self._require_auth():
                    return
                url = qs.get("url", [None])[0]
                if not url:
                    return self._send(400, json.dumps({"success": False, "message": "URL manquante"}).encode(), "application/json")
                parsed = urllib.parse.urlsplit(url)
                if parsed.scheme not in ("http", "https") or not parsed.netloc:
                    return self._send(400, json.dumps({"success": False,
                                      "message": "URL invalide (http/https uniquement)"}).encode(), "application/json")

                added_channels, message = scrape_channels_from_url(url)

                return self._send(200, json.dumps({
                    "success": True,
                    "message": message,
                    "added": len(added_channels),
                    "channels": added_channels
                }).encode(), "application/json")

            if path == "/api/refresh-cache":
                if not self._require_auth():
                    return
                t0 = time.time()
                _ch_cache["at"] = 0.0
                n_dl = len(channels())
                _vavoo_cache["at"] = 0.0
                n_vv = len(vavoo_channels())
                dur_ms = int((time.time() - t0) * 1000)
                _log_activity("Cache rafraîchi", f"{n_dl} dlstreams / {n_vv} vavoo en {dur_ms}ms")
                return self._send(200, json.dumps({"success": True, "dlstreams": n_dl, "vavoo": n_vv,
                                                    "ms": dur_ms}).encode(), "application/json")

            if path in ("/", "/manifest.json"):
                lang = qs.get("lang", [None])[0]
                return self._send(200, json.dumps(self._manifest(lang_filter=lang)).encode(), "application/json", True)

            if path.startswith("/catalog/tv/"):
                extra = path[len("/catalog/tv/"):].removesuffix(".json")
                catid = extra.split("/", 1)[0]
                params = {}
                if "/" in extra:
                    for kv in extra.split("/", 1)[1].split("&"):
                        if "=" in kv:
                            k, v = kv.split("=", 1)
                            params[k] = urllib.parse.unquote_plus(v)
                
                lang_filter = qs.get("lang", [None])[0]
                
                chans = vavoo_channels() if catid == "vavoo" else channels(lang_filter=lang_filter)
                q = params.get("search", "").lower().strip()
                if q:
                    words = q.replace("+", " ").split()
                    chans = [c for c in chans if all(w in c["name"].lower() for w in words)]
                skip = int(params.get("skip") or 0)
                metas = [self._meta(c, catid) for c in chans[skip:skip + 100]]
                return self._send(200, json.dumps({"metas": metas}).encode(), "application/json", True)

            if path.startswith("/meta/tv/"):
                seg = urllib.parse.unquote(path.rsplit("/", 1)[1].removesuffix(".json"))
                source, _, cid = seg.partition(":")
                if source == "vavoo":
                    url = _unb64u(cid)
                    c = next((x for x in vavoo_channels() if x["id"] == url), {"id": url, "name": "Vavoo"})
                else:
                    c = next((x for x in channels() if x["id"] == cid), {"id": cid, "name": f"dlstreams {cid}"})
                return self._send(200, json.dumps({"meta": self._meta(c, source)}).encode(),
                                  "application/json", True)

            if path.startswith("/stream/tv/"):
                seg = urllib.parse.unquote(path.rsplit("/", 1)[1].removesuffix(".json"))
                source, _, cid = seg.partition(":")
                b = self._self_base()
                if source == "vavoo":
                    streams = [{"name": "Vavoo", "title": "📺 Direct", "url": f"{b}/vhls?v={cid}"}]
                    return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")
                ok = working_players(cid)
                streams = [{"name": "dlstreams", "title": "🔀 Auto (1er dispo)",
                            "url": f"{b}/hls/{cid}/index.m3u8"}] if ok else []
                streams += [{"name": "dlstreams", "title": label,
                             "url": f"{b}/hls/{cid}/p{i}/index.m3u8"} for i, label in ok]
                return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")

            if path.startswith("/hls/") and path.endswith("/index.m3u8"):
                parts = path.split("/")
                cid = parts[2]
                if len(parts) == 5 and parts[3].startswith("p") and parts[3][1:].isdigit():
                    pls = players(cid)
                    idx = int(parts[3][1:])
                    if idx >= len(pls):
                        return self._send(404, b"player inconnu", "text/plain")
                    m3u8, host = resolve_player(pls[idx][1])
                else:
                    m3u8, host = resolve(cid)
                hdr = {"Referer": host + "/", "Origin": host}
                henc = _b64u(json.dumps(hdr))
                text = _proxy_get(m3u8, hdr).decode("utf-8", "replace")
                return self._send(200, _rewrite_playlist(text, m3u8, henc, self._self_base()).encode(),
                                  "application/vnd.apple.mpegurl")

            if path == "/vhls":
                real = vavoo_resolve(_unb64u(qs["v"][0]))
                if not real:
                    return self._send(502, b"vavoo: flux introuvable (hors-antenne ?)", "text/plain")
                hdr = {"User-Agent": _VAVOO_UA}
                henc = _b64u(json.dumps(hdr))
                text = _proxy_get(real, hdr).decode("utf-8", "replace")
                return self._send(200, _rewrite_playlist(text, real, henc, self._self_base()).encode(),
                                  "application/vnd.apple.mpegurl")

            if path == "/px":
                url = _unb64u(qs["u"][0])
                henc = qs.get("h", [""])[0]
                hdr = json.loads(_unb64u(henc)) if henc else {}
                text = _proxy_get(url, hdr).decode("utf-8", "replace")
                return self._send(200, _rewrite_playlist(text, url, henc, self._self_base()).encode(),
                                  "application/vnd.apple.mpegurl")

            if path == "/sx":
                url = _unb64u(qs["u"][0])
                henc = qs.get("h", [""])[0]
                hdr = json.loads(_unb64u(henc)) if henc else {}
                return self._send(200, _proxy_get(url, hdr), "video/mp2t")

            return self._send(404, b"not found", "text/plain")

        except Exception as e:
            return self._send(502, f"resolve/proxy error: {type(e).__name__}: {e}".encode(), "text/plain")

    def _manifest(self, lang_filter: str | None = None) -> dict:
        _extra = [{"name": "search", "isRequired": False}, {"name": "skip", "isRequired": False}]
        name = "dlstreams + Vavoo"
        desc = "Chaines live dlstreams (DaddyLive) + Vavoo, proxifiees (headers + content-type corriges). Dashboard web integre avec filtre langue. Sans MediaFlow."
        
        if lang_filter and lang_filter != "all":
            lang_names = {"fr": "Français", "en": "English", "es": "Español", "de": "Deutsch", "it": "Italiano", "ar": "Arabe", "pt": "Português"}
            lang_name = lang_names.get(lang_filter, lang_filter)
            name = f"dlstreams {lang_name}"
            desc = f"Chaines {lang_name} uniquement. Filtre actif: {lang_name}."
        
        return {
            "id": "st.dlstreams.proxy" + (f".{lang_filter}" if lang_filter and lang_filter != "all" else ""),
            "version": "1.5.0",
            "name": name,
            "description": desc,
            "resources": ["catalog", "meta", "stream"],
            "types": ["tv"],
            "idPrefixes": ["dlstreams:", "vavoo:"],
            "catalogs": [{"type": "tv", "id": "dlstreams", "name": "dlstreams",
                          "extra": _extra, "extraSupported": ["search", "skip"]},
                         {"type": "tv", "id": "vavoo", "name": "Vavoo",
                          "extra": _extra, "extraSupported": ["search", "skip"]}],
        }

    def _meta(self, c: dict, source: str) -> dict:
        cid = c["id"] if source == "dlstreams" else _b64u(c["id"])
        logo = c.get("logo") or ""
        return {"id": f"{source}:{cid}", "type": "tv", "name": c["name"],
                "poster": logo, "logo": logo, "posterShape": "landscape"}

def main():
    print(f"dlstreams addon+proxy sur http://0.0.0.0:{PORT}")
    print(f"  Dashboard: http://127.0.0.1:{PORT}/dashboard")
    print(f"  Configure: http://127.0.0.1:{PORT}/configure")
    print(f"  Stremio  : http://<ton-ip-LAN>:{PORT}/manifest.json?lang=fr")
    print(f"  VLC/mpv  : http://127.0.0.1:{PORT}/hls/121/index.m3u8")
    if _PASSWORD_GENERATED:
        print("  " + "=" * 62)
        print(f"  Mot de passe dashboard (genere automatiquement) : {DASHBOARD_PASSWORD}")
        print("  Il changera au prochain redemarrage. Definis la variable")
        print("  d'environnement DASHBOARD_PASSWORD sur Render pour en garder un stable.")
        print("  " + "=" * 62)
    else:
        print("  Mot de passe dashboard : defini via DASHBOARD_PASSWORD")
    try:
        n = len(channels())
        print(f"  annuaire : {n} chaines chargees (dont {len(_POPULAR_CHANNELS)} populaires)")
    except Exception as e:
        print(f"  annuaire : erreur de chargement ({e})")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

DASHBOARD_HTML = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>dlstreams — Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.15/dist/hls.min.js"></script>
<style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    :root {
        --primary: #6366f1;
        --primary-dark: #4f46e5;
        --secondary: #ec4899;
        --bg-dark: #0f172a;
        --bg-card: #1e293b;
        --bg-hover: #334155;
        --text-primary: #f1f5f9;
        --text-secondary: #94a3b8;
        --border: #334155;
        --success: #10b981;
        --warning: #f59e0b;
        --error: #ef4444;
        --gradient: linear-gradient(135deg, #6366f1 0%, #ec4899 100%);
    }
    body {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        background: var(--bg-dark);
        background-image: 
            radial-gradient(circle at 20% 50%, rgba(99, 102, 241, 0.15) 0%, transparent 50%),
            radial-gradient(circle at 80% 80%, rgba(236, 72, 153, 0.15) 0%, transparent 50%);
        color: var(--text-primary);
        line-height: 1.6;
        min-height: 100vh;
    }
    .login-screen {
        min-height: 100vh;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .login-container {
        background: rgba(30, 41, 59, 0.8);
        backdrop-filter: blur(20px);
        border: 1px solid var(--border);
        border-radius: 24px;
        padding: 48px;
        width: 90%;
        max-width: 420px;
        box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
        animation: slideUp 0.5s ease-out;
    }
    .login-logo { text-align: center; margin-bottom: 32px; }
    .login-logo .logo-icon {
        width: 80px; height: 80px; margin: 0 auto 16px;
        background: var(--gradient); border-radius: 20px;
        display: grid; place-items: center; font-size: 40px;
        font-weight: 800; color: #0b0f1a;
        box-shadow: 0 8px 24px rgba(99, 102, 241, 0.4);
    }
    .login-logo h1 {
        font-size: 28px; font-weight: 700;
        background: var(--gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .login-form input {
        width: 100%; padding: 14px 18px;
        background: var(--bg-dark); border: 2px solid var(--border);
        border-radius: 12px; color: var(--text-primary);
        font-size: 15px; transition: all 0.3s; margin-bottom: 16px;
    }
    .login-form input:focus {
        outline: none; border-color: var(--primary);
        box-shadow: 0 0 0 4px rgba(99, 102, 241, 0.1);
    }
    .login-form button {
        width: 100%; padding: 14px; background: var(--gradient);
        border: none; border-radius: 12px; color: white;
        font-weight: 600; font-size: 15px; cursor: pointer;
        transition: all 0.3s; box-shadow: 0 4px 12px rgba(99, 102, 241, 0.3);
    }
    .login-form button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(99, 102, 241, 0.4);
    }
    .error-message {
        background: rgba(239, 68, 68, 0.1); border: 1px solid var(--error);
        color: var(--error); padding: 12px; border-radius: 8px;
        margin-bottom: 16px; text-align: center; animation: shake 0.5s;
    }
    .dashboard-container { display: none; }
    .dashboard-container.active { display: flex; }
    .sidebar {
        position: fixed; left: 0; top: 0; bottom: 0; width: 280px;
        background: var(--bg-card); border-right: 1px solid var(--border);
        padding: 24px; overflow-y: auto; z-index: 100;
        display: flex; flex-direction: column;
    }
    .sidebar-header {
        display: flex; align-items: center; gap: 12px;
        padding-bottom: 24px; border-bottom: 1px solid var(--border);
        margin-bottom: 24px;
    }
    .sidebar-header .logo {
        width: 40px; height: 40px; border-radius: 10px;
        background: var(--gradient); display: grid; place-items: center;
        font-weight: 800; color: #0b0f1a; font-size: 20px;
    }
    .sidebar-header h2 {
        font-size: 18px; font-weight: 700;
        background: var(--gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .nav-section { margin-bottom: 24px; }
    .nav-section-title {
        font-size: 11px; text-transform: uppercase;
        letter-spacing: 0.05em; color: var(--text-secondary);
        margin-bottom: 12px; padding-left: 12px; font-weight: 600;
    }
    .nav-item {
        display: flex; align-items: center; gap: 12px;
        padding: 12px; border-radius: 10px; cursor: pointer;
        transition: all 0.2s; margin-bottom: 4px;
        color: var(--text-secondary); text-decoration: none; font-size: 14px;
    }
    .nav-item:hover { background: var(--bg-hover); color: var(--text-primary); }
    .nav-item.active {
        background: rgba(99, 102, 241, 0.15);
        color: var(--primary); font-weight: 500;
    }
    .nav-item .icon { font-size: 18px; }
    .logout-btn {
        margin-top: auto; padding: 12px; border-radius: 10px;
        background: rgba(239, 68, 68, 0.1); border: 1px solid var(--error);
        color: var(--error); cursor: pointer; transition: all 0.2s;
        display: flex; align-items: center; gap: 12px; font-size: 14px;
    }
    .logout-btn:hover { background: rgba(239, 68, 68, 0.2); }
    .main-content { margin-left: 280px; padding: 32px; flex: 1; }
    .top-bar {
        display: flex; justify-content: space-between; align-items: center;
        margin-bottom: 32px; padding-bottom: 24px;
        border-bottom: 1px solid var(--border);
    }
    .page-title h1 {
        font-size: 28px; font-weight: 700; margin-bottom: 4px;
        background: var(--gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .page-title p { color: var(--text-secondary); font-size: 14px; }
    .refresh-btn {
        padding: 10px 20px; background: var(--bg-card);
        border: 1px solid var(--border); border-radius: 10px;
        color: var(--text-primary); cursor: pointer; transition: all 0.2s;
        display: flex; align-items: center; gap: 8px; font-size: 14px;
    }
    .refresh-btn:hover { background: var(--bg-hover); border-color: var(--primary); }
    .stats-grid {
        display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        gap: 20px; margin-bottom: 32px;
    }
    .stat-card {
        background: var(--bg-card); border: 1px solid var(--border);
        border-radius: 16px; padding: 24px; transition: all 0.3s;
        position: relative; overflow: hidden; animation: slideUp 0.5s ease-out;
    }
    .stat-card::before {
        content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
        background: var(--gradient); opacity: 0; transition: opacity 0.3s;
    }
    .stat-card:hover {
        transform: translateY(-4px); border-color: var(--primary);
        box-shadow: 0 12px 24px rgba(0, 0, 0, 0.3);
    }
    .stat-card:hover::before { opacity: 1; }
    .stat-label { font-size: 13px; color: var(--text-secondary); margin-bottom: 8px; }
    .stat-value {
        font-size: 32px; font-weight: 700;
        background: var(--gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 8px;
    }
    .stat-hint { font-size: 12px; color: var(--text-secondary); }
    .card {
        background: var(--bg-card); border: 1px solid var(--border);
        border-radius: 16px; padding: 24px; margin-bottom: 24px;
        animation: slideUp 0.5s ease-out;
    }
    .card h2 {
        font-size: 18px; font-weight: 600; margin-bottom: 20px;
        display: flex; align-items: center; gap: 10px;
    }
    .add-source-box {
        background: var(--bg-dark); border: 2px dashed var(--border);
        border-radius: 12px; padding: 24px; margin-bottom: 20px;
        transition: all 0.3s;
    }
    .add-source-box:hover { border-color: var(--primary); }
    .add-source-input {
        width: 100%; background: var(--bg-card); border: 2px solid var(--border);
        border-radius: 10px; padding: 12px 16px; color: var(--text-primary);
        font-size: 14px; margin-bottom: 12px; transition: all 0.3s;
    }
    .add-source-input:focus {
        outline: none; border-color: var(--primary);
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
    }
    .add-source-btn {
        padding: 12px 24px; background: var(--gradient);
        border: none; border-radius: 10px; color: white;
        font-weight: 600; font-size: 14px; cursor: pointer;
        transition: all 0.2s; box-shadow: 0 4px 12px rgba(99, 102, 241, 0.3);
    }
    .add-source-btn:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(99, 102, 241, 0.4);
    }
    .add-source-btn:disabled { opacity: 0.6; cursor: not-allowed; transform: none; }
    .add-source-result { margin-top: 12px; font-size: 13px; }
    .manual-channels-list {
        display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
        gap: 12px; margin-top: 16px;
    }
    .manual-channel-item {
        display: flex; align-items: center; justify-content: space-between;
        padding: 12px; background: var(--bg-dark); border: 1px solid var(--border);
        border-radius: 10px; transition: all 0.2s;
    }
    .manual-channel-item:hover { border-color: var(--primary); }
    .manual-channel-info { flex: 1; }
    .manual-channel-name { font-size: 14px; font-weight: 500; margin-bottom: 4px; }
    .manual-channel-meta { font-size: 11px; color: var(--text-secondary); }
    .remove-btn {
        padding: 6px 12px; background: rgba(239, 68, 68, 0.1);
        border: 1px solid var(--error); border-radius: 6px;
        color: var(--error); cursor: pointer; font-size: 12px;
        transition: all 0.2s;
    }
    .remove-btn:hover { background: rgba(239, 68, 68, 0.2); }
    .search-bar {
        display: flex; gap: 10px; align-items: center;
        flex-wrap: wrap; margin-bottom: 20px;
    }
    .search-bar input {
        flex: 1; min-width: 200px; background: var(--bg-dark);
        border: 2px solid var(--border); border-radius: 10px;
        padding: 12px 16px; color: var(--text-primary); font-size: 14px;
    }
    .search-bar input:focus {
        outline: none; border-color: var(--primary);
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
    }
    .search-bar select {
        background: var(--bg-dark); border: 2px solid var(--border);
        border-radius: 10px; padding: 12px 16px;
        color: var(--text-primary); cursor: pointer;
    }
    .tabs { display: flex; gap: 6px; }
    .tab {
        padding: 8px 14px; border-radius: 8px; border: 1px solid var(--border);
        background: transparent; color: var(--text-secondary);
        cursor: pointer; transition: all 0.2s; font-size: 13px;
    }
    .tab.active {
        background: var(--gradient); color: #0b0f1a;
        border-color: transparent; font-weight: 600;
    }
    .channel-list {
        display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
        gap: 10px;
    }
    .channel-item {
        display: flex; align-items: center; gap: 10px; padding: 12px;
        border: 1px solid var(--border); border-radius: 10px;
        background: var(--bg-dark); cursor: pointer; transition: all 0.2s;
        text-decoration: none; color: var(--text-primary);
    }
    .channel-item:hover {
        border-color: var(--primary); transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(99, 102, 241, 0.2);
    }
    .channel-item .name {
        flex: 1; font-size: 13px;
        overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .channel-item .id {
        color: var(--text-secondary); font-size: 11px;
        font-family: ui-monospace, monospace;
    }
    .access-grid {
        display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
        gap: 14px;
    }
    .access-card {
        background: var(--bg-dark); border: 1px solid var(--border);
        border-radius: 12px; padding: 16px;
    }
    .access-card .label {
        font-size: 12px; color: var(--text-secondary);
        text-transform: uppercase; letter-spacing: 0.05em;
        margin-bottom: 8px; font-weight: 600;
    }
    .access-card a {
        color: var(--primary); text-decoration: none;
        word-break: break-all; font-size: 13px;
    }
    .access-card a:hover { color: var(--secondary); }
    .copy-btn {
        display: inline-flex; align-items: center; gap: 6px;
        margin-top: 8px; padding: 6px 12px; border: 1px solid var(--border);
        border-radius: 8px; background: transparent; color: var(--text-secondary);
        font-size: 12px; cursor: pointer; transition: all 0.2s;
    }
    .copy-btn:hover { color: var(--text-primary); border-color: var(--primary); }
    .player-modal {
        position: fixed; top: 0; left: 0; right: 0; bottom: 0;
        background: rgba(0,0,0,0.95); z-index: 1000;
        display: none; align-items: center; justify-content: center; padding: 20px;
    }
    .player-modal.active { display: flex; }
    .player-container {
        width: 100%; max-width: 1200px; background: var(--bg-card);
        border-radius: 16px; overflow: hidden;
        box-shadow: 0 20px 60px rgba(0,0,0,0.5);
    }
    .player-header {
        display: flex; justify-content: space-between; align-items: center;
        padding: 16px 20px; border-bottom: 1px solid var(--border);
    }
    .player-header h3 { margin: 0; font-size: 16px; }
    .player-close {
        background: none; border: none; color: var(--text-secondary);
        font-size: 24px; cursor: pointer; width: 32px; height: 32px;
        display: grid; place-items: center; border-radius: 6px; transition: all 0.2s;
    }
    .player-close:hover { background: var(--bg-hover); color: var(--text-primary); }
    .player-body { padding: 20px; }
    .player-frame {
        width: 100%; aspect-ratio: 16/9; background: #000;
        border-radius: 8px; border: none;
    }
    .alert {
        padding: 10px 14px; border-radius: 8px; margin-top: 8px; font-size: 12px;
    }
    .alert-success {
        background: rgba(16, 185, 129, 0.15); border: 1px solid var(--success);
        color: var(--success);
    }
    .alert-error {
        background: rgba(239, 68, 68, 0.15); border: 1px solid var(--error);
        color: var(--error);
    }
    .toast {
        position: fixed; bottom: 20px; right: 20px;
        background: var(--bg-card); border: 1px solid var(--border);
        border-radius: 12px; padding: 16px 20px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.3); z-index: 2000;
        animation: slideInRight 0.3s ease-out;
        display: flex; align-items: center; gap: 12px;
    }
    .toast.success { border-color: var(--success); }
    .toast.error { border-color: var(--error); }
    .status-indicator {
        display: flex; align-items: center; gap: 8px;
        padding: 6px 12px; border-radius: 20px;
        background: rgba(16, 185, 129, 0.1); border: 1px solid var(--success);
        color: var(--success); font-size: 12px; font-weight: 500;
    }
    .status-dot {
        width: 8px; height: 8px; border-radius: 50%;
        background: var(--success); animation: pulse 2s infinite;
    }
    .page { display: none; }
    .page.active { display: block; }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    @keyframes slideUp {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    @keyframes slideInRight {
        from { opacity: 0; transform: translateX(100px); }
        to { opacity: 1; transform: translateX(0); }
    }
    @keyframes shake {
        0%, 100% { transform: translateX(0); }
        25% { transform: translateX(-10px); }
        75% { transform: translateX(10px); }
    }
    .stat-card .stat-icon {
        position: absolute; top: 18px; right: 18px;
        font-size: 22px; opacity: 0.9;
    }
    .stat-card.accent-dl::before { background: linear-gradient(135deg, #6366f1, #818cf8); }
    .stat-card.accent-vv::before { background: linear-gradient(135deg, #ec4899, #f472b6); }
    .stat-card.accent-manual::before { background: linear-gradient(135deg, #10b981, #34d399); }
    .stat-card.accent-up::before { background: linear-gradient(135deg, #f59e0b, #fbbf24); }
    .stat-card.accent-req::before { background: linear-gradient(135deg, #0ea5e9, #38bdf8); }
    .stat-card.accent-err::before { background: linear-gradient(135deg, #ef4444, #f87171); }
    .cache-badge {
        display: inline-flex; align-items: center; gap: 6px;
        font-size: 11px; padding: 3px 10px; border-radius: 20px;
        border: 1px solid var(--border); color: var(--text-secondary);
    }
    .cache-badge .dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
    .cache-badge.ok { color: var(--success); border-color: rgba(16,185,129,.4); background: rgba(16,185,129,.08); }
    .cache-badge.stale { color: var(--warning); border-color: rgba(245,158,11,.4); background: rgba(245,158,11,.08); }
    .cache-badge.old { color: var(--error); border-color: rgba(239,68,68,.4); background: rgba(239,68,68,.08); }
    .fav-star {
        font-size: 16px; color: var(--text-secondary); cursor: pointer;
        transition: transform .2s, color .2s; user-select: none; padding: 2px 6px;
    }
    .fav-star:hover { transform: scale(1.25); color: var(--warning); }
    .fav-star.active { color: var(--warning); text-shadow: 0 0 12px rgba(245,158,11,.6); }
    .chart { display: flex; flex-direction: column; gap: 10px; margin-top: 8px; }
    .chart-row { display: grid; grid-template-columns: 110px 1fr 48px; align-items: center; gap: 12px; }
    .chart-label { font-size: 13px; color: var(--text-secondary); text-align: right; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .chart-bar { height: 22px; background: var(--bg-dark); border-radius: 6px; overflow: hidden; }
    .chart-fill {
        height: 100%; border-radius: 6px; width: 0;
        background: var(--gradient);
        transition: width .8s cubic-bezier(.22, 1, .36, 1);
    }
    .chart-count { font-size: 12px; color: var(--text-secondary); text-align: right; font-variant-numeric: tabular-nums; }
    .update-time {
        font-size: 12px; color: var(--text-secondary);
        display: flex; align-items: center; gap: 8px; white-space: nowrap;
    }
    .update-time .spinner {
        width: 13px; height: 13px; border: 2px solid var(--border);
        border-top-color: var(--primary); border-radius: 50%;
        animation: spin .8s linear infinite; opacity: 0;
    }
    .update-time.loading .spinner { opacity: 1; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .mini-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 10px; }
    .fav-empty { grid-column: 1/-1; text-align: center; padding: 30px; color: var(--text-secondary); }
    .fav-empty a { color: var(--primary); }
    @media (max-width: 768px) {
        .sidebar { transform: translateX(-100%); }
        .sidebar.open { transform: translateX(0); }
        .main-content { margin-left: 0; padding: 20px; }
        .stats-grid { grid-template-columns: 1fr; }
    }
</style>
</head>
<body>
<div class="login-screen" id="loginScreen">
    <div class="login-container">
        <div class="login-logo">
            <div class="logo-icon">▶</div>
            <h1>dlstreams</h1>
        </div>
        <div id="loginError"></div>
        <form class="login-form" onsubmit="handleLogin(event)">
            <input type="password" id="passwordInput" placeholder="Mot de passe" required>
            <button type="submit">Se connecter</button>
        </form>
    </div>
</div>

<div class="dashboard-container" id="dashboard">
    <aside class="sidebar">
        <div class="sidebar-header">
            <div class="logo">▶</div>
            <h2>dlstreams</h2>
        </div>
        <div class="nav-section">
            <div class="nav-section-title">Navigation</div>
            <a class="nav-item active" data-page="dashboard" onclick="navigateTo('dashboard')"><span class="icon">📊</span> Dashboard</a>
            <a class="nav-item" data-page="catalog" onclick="navigateTo('catalog')"><span class="icon">📺</span> Catalogue</a>
            <a class="nav-item" data-page="sources" onclick="navigateTo('sources')"><span class="icon">📡</span> Sources</a>
        </div>
        <div class="nav-section">
            <div class="nav-section-title">Liens</div>
            <a href="/configure" class="nav-item"><span class="icon">🎨</span> Configuration</a>
        </div>
        <button class="logout-btn" onclick="logout()">
            <span class="icon">🚪</span> Déconnexion
        </button>
    </aside>

    <main class="main-content">
        <div class="top-bar">
            <div class="page-title">
                <h1 id="pageTitle">Dashboard</h1>
                <p id="pageSubtitle">Vue d'ensemble de votre addon</p>
            </div>
            <div style="display: flex; gap: 12px; align-items: center;">
                <div class="update-time" id="update-time">
                    <div class="spinner"></div>
                    <span id="update-label">Mise à jour…</span>
                </div>
                <div class="status-indicator">
                    <div class="status-dot"></div>
                    <span>En ligne</span>
                </div>
                <button class="refresh-btn" onclick="refreshAll()">
                    <span>↻</span> Actualiser
                </button>
            </div>
        </div>

        <!-- Page Dashboard -->
        <div class="page active" id="page-dashboard">
            <section class="stats-grid">
                <div class="stat-card accent-dl">
                    <div class="stat-icon">📡</div>
                    <div class="stat-label">Chaînes dlstreams</div>
                    <div class="stat-value" id="c-dl">—</div>
                    <div class="stat-hint" id="c-dl-h"><span class="cache-badge"><span class="dot"></span>chargement…</span></div>
                </div>
                <div class="stat-card accent-vv">
                    <div class="stat-icon">📺</div>
                    <div class="stat-label">Chaînes Vavoo</div>
                    <div class="stat-value" id="c-vv">—</div>
                    <div class="stat-hint" id="c-vv-h"><span class="cache-badge"><span class="dot"></span>chargement…</span></div>
                </div>
                <div class="stat-card accent-manual">
                    <div class="stat-icon">➕</div>
                    <div class="stat-label">Sources manuelles</div>
                    <div class="stat-value" id="c-manual">0</div>
                    <div class="stat-hint">ajoutées par vous</div>
                </div>
                <div class="stat-card accent-up">
                    <div class="stat-icon">⏱️</div>
                    <div class="stat-label">Uptime</div>
                    <div class="stat-value" id="c-up">—</div>
                    <div class="stat-hint">depuis démarrage</div>
                </div>
                <div class="stat-card accent-req">
                    <div class="stat-icon">🔁</div>
                    <div class="stat-label">Requêtes</div>
                    <div class="stat-value" id="c-req">—</div>
                    <div class="stat-hint">total depuis démarrage</div>
                </div>
                <div class="stat-card accent-err">
                    <div class="stat-icon">⚠️</div>
                    <div class="stat-label">Erreurs</div>
                    <div class="stat-value" id="c-err">—</div>
                    <div class="stat-hint">requêtes en échec</div>
                </div>
            </section>

            <section class="card">
                <h2>⭐ Favoris <span style="font-size:13px;color:var(--text-secondary);font-weight:400">— épinglés depuis le catalogue</span></h2>
                <div class="search-bar">
                    <input id="fav-q" type="search" placeholder="Filtrer mes favoris…">
                </div>
                <div class="mini-grid" id="fav-list">
                    <div class="fav-empty">Aucun favori — va dans le <a href="#" onclick="navigateTo('catalog');return false">Catalogue</a> et clique sur ★ pour épingler une chaîne</div>
                </div>
            </section>

            <section class="card">
                <h2>📈 Répartition par langue</h2>
                <div class="chart" id="lang-chart"></div>
            </section>

            <section class="card">
                <h2>🕒 Activité récente</h2>
                <div id="activity-list" style="font-size:13px;color:var(--text-secondary)">chargement…</div>
            </section>

            <section class="card">
                <h2>📱 Accès rapide</h2>
                <div class="access-grid">
                    <div class="access-card">
                        <div class="label">Stremio — installer l'addon</div>
                        <div style="margin-top:6px;font-size:13px;color:var(--text-secondary)">Addons → « Install via URL »</div>
                        <a id="manifest" href="#">—</a>
                        <button class="copy-btn" data-copy="manifest">📋 copier l'URL</button>
                    </div>
                    <div class="access-card">
                        <div class="label">VLC / mpv / ffmpeg — lecture directe</div>
                        <div style="margin-top:6px;font-size:13px;color:var(--text-secondary)">Ouvre un flux par son id :</div>
                        <code id="vlc" style="color:var(--primary);word-break:break-all;font-size:12px">—</code>
                        <button class="copy-btn" data-copy="vlc">📋 copier</button>
                    </div>
                    <div class="access-card">
                        <div class="label">Endpoints de l'API</div>
                        <div style="margin-top:6px;font-size:12px;color:var(--text-secondary)">
                            <div><code>/manifest.json</code> — Stremio</div>
                            <div><code>/api/channels</code> — annuaire</div>
                            <div><code>/configure</code> — config langue</div>
                        </div>
                    </div>
                </div>
            </section>
        </div>

        <!-- Page Sources -->
        <div class="page" id="page-sources">
            <section class="card">
                <h2>📡 Ajouter une source</h2>
                <div class="add-source-box">
                    <input class="add-source-input" id="source-url" type="url" placeholder="Collez l'URL d'une page dlstreams (ex: https://dlstreams.st/watch.php?id=121)">
                    <button class="add-source-btn" id="add-source-btn">🔍 Scraper & Ajouter</button>
                    <div class="add-source-result" id="add-source-result"></div>
                </div>
            </section>

            <section class="card">
                <h2>📋 Sources ajoutées manuellement</h2>
                <p style="color:var(--text-secondary);font-size:13px;margin-bottom:16px">Ces chaînes ont été ajoutées via le scraper et sont conservées en mémoire.</p>
                <div class="manual-channels-list" id="manual-channels-list">
                    <div style="color:var(--text-secondary);text-align:center;padding:30px;grid-column:1/-1">Aucune source ajoutée</div>
                </div>
            </section>

            <section class="card">
                <h2>🕒 Activité récente</h2>
                <div id="activity-list" style="font-size:13px;color:var(--text-secondary)">chargement…</div>
            </section>
        </div>

        <!-- Page Catalogue -->
        <div class="page" id="page-catalog">
            <section class="card">
                <h2>📺 Catalogue complet</h2>
                <div class="search-bar">
                    <input id="q" type="search" placeholder="Rechercher une chaîne (ex : beIN, Canal+, RMC Sport…)">
                    <select id="lang-filter">
                        <option value="all">🌍 Toutes langues</option>
                        <option value="fr" selected>🇫🇷 Français</option>
                        <option value="en">🇬🇧 English</option>
                        <option value="es">🇪🇸 Español</option>
                        <option value="de">🇩🇪 Deutsch</option>
                        <option value="it">🇮🇹 Italiano</option>
                        <option value="ar">🇸🇦 Arabe</option>
                        <option value="pt">🇵🇹 Português</option>
                        <option value="other">📺 Autres</option>
                    </select>
                    <div class="tabs">
                        <button class="tab active" data-src="dlstreams">dlstreams</button>
                        <button class="tab" data-src="vavoo">Vavoo</button>
                    </div>
                </div>
                <div class="channel-list" id="list"><div style="color:var(--text-secondary);text-align:center;padding:30px;grid-column:1/-1">chargement…</div></div>
            </section>
        </div>

        <footer style="margin-top:40px;color:var(--text-secondary);font-size:12px;text-align:center">
            dlstreams addon+proxy · Python stdlib pure · zéro dépendance · <span id="host"></span>
        </footer>
    </main>
</div>

<div class="player-modal" id="player-modal">
    <div class="player-container">
        <div class="player-header">
            <h3 id="player-title">Lecture</h3>
            <button class="player-close" id="player-close">×</button>
        </div>
        <div class="player-body">
            <video class="player-frame" id="player-frame" controls autoplay></video>
        </div>
    </div>
</div>

<script>
const BASE = location.origin;
const $ = s => document.querySelector(s);
const fmtDur = s => {
    if (s==null) return "—";
    const d=Math.floor(s/86400), h=Math.floor(s%86400/3600), m=Math.floor(s%3600/60);
    return (d?d+"j ":"")+(h?h+"h ":"")+(m+"m");
};
const fmtAge = s => s==null ? "pas encore chargé" : (s<60?s+"s":Math.floor(s/60)+"min");

// Session reelle : verifiee cote serveur via le cookie httponly (plus de
// localStorage -- n'importe qui pouvait s'auto-connecter depuis la console
// sans jamais connaitre le mot de passe).
async function checkSession() {
    try {
        const r = await fetch("/api/stats");
        if (r.ok) {
            $('#loginScreen').style.display = 'none';
            $('#dashboard').classList.add('active');
            await boot();
        }
    } catch (e) { /* pas connecte : ecran de login reste affiche */ }
}

// Wrapper fetch : si la session a expire en cours d'usage, renvoie a l'ecran
// de login au lieu d'afficher des erreurs silencieuses.
async function apiFetch(url, opts) {
    const r = await fetch(url, opts);
    if (r.status === 401) {
        $('#dashboard').classList.remove('active');
        $('#loginScreen').style.display = 'flex';
        showToast('Session expirée, reconnecte-toi', 'error');
        throw new Error('unauthenticated');
    }
    return r;
}

function showToast(message, type = 'success') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);
    setTimeout(() => toast.remove(), 3000);
}

function handleLogin(e) {
    e.preventDefault();
    const password = $('#passwordInput').value;
    fetch('/api/auth', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({password})
    })
        .then(r => r.json().then(data => ({ok: r.ok, data})))
        .then(({ok, data}) => {
            if (ok && data.success) {
                $('#loginError').innerHTML = '';
                $('#loginScreen').style.display = 'none';
                $('#dashboard').classList.add('active');
                showToast('✅ Connecté avec succès');
                boot();
            } else {
                $('#loginError').innerHTML = `<div class="error-message">${data.message || 'Mot de passe incorrect'}</div>`;
            }
        })
        .catch(() => { $('#loginError').innerHTML = '<div class="error-message">Erreur réseau</div>'; });
}

function logout() {
    fetch('/api/logout').finally(() => {
        $('#dashboard').classList.remove('active');
        $('#loginScreen').style.display = 'flex';
        $('#passwordInput').value = '';
        showToast('👋 Déconnecté');
    });
}

function navigateTo(page) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    
    $(`#page-${page}`).classList.add('active');
    document.querySelector(`[data-page="${page}"]`).classList.add('active');
    
    const titles = {
        dashboard: ['Dashboard', 'Vue d\'ensemble de votre addon'],
        sources: ['Sources', 'Gérer vos sources personnalisées'],
        catalog: ['Catalogue', 'Explorer toutes les chaînes disponibles']
    };
    
    $('#pageTitle').textContent = titles[page][0];
    $('#pageSubtitle').textContent = titles[page][1];
    
    if (page === 'dashboard') { renderFavs(); loadActivity(); }
    if (page === 'sources') { loadManualChannels(); loadActivity(); }
    if (page === 'catalog') {
        if (!ALL.dlstreams.length) loadCatalog('dlstreams');
        render();
    }
}

async function refreshStats(){
    try{
        const r = await apiFetch("/api/stats");
        const d = await r.json();
        $("#c-dl").textContent = d.dlstreams.count;
        setCacheBadge($("#c-dl-h"), d.dlstreams.age_seconds);
        $("#c-vv").textContent = d.vavoo.count;
        setCacheBadge($("#c-vv-h"), d.vavoo.age_seconds);
        $("#c-up").textContent = fmtDur(d.uptime);
        $("#c-manual").textContent = d.manual_channels || 0;
        $("#c-req").textContent = (d.requests||0).toLocaleString('fr-FR');
        $("#c-err").textContent = d.errors || 0;
        renderChart(d.lang_counts || {});
        $("#update-time").classList.remove("loading");
        $("#update-label").textContent = "MAJ " + new Date().toLocaleTimeString('fr-FR');
    }catch(e){
        if (e.message !== 'unauthenticated') console.error("Stats error:", e);
    }
}

function setCacheBadge(el, age){
    if(age == null){ el.innerHTML = '<span class="cache-badge old"><span class="dot"></span>pas encore chargé</span>'; return; }
    let cls = "ok", label = "cache : il y a " + fmtAge(age);
    if(age > 3600){ cls = "old"; label = "périmé (" + fmtAge(age) + ")"; }
    else if(age > 600){ cls = "stale"; }
    el.innerHTML = `<span class="cache-badge ${cls}"><span class="dot"></span>${label}</span>`;
}

function renderChart(lang_counts){
    const el = $("#lang-chart");
    if(!el) return;
    const flags = {fr:"🇫🇷",en:"🇬🇧",es:"🇪🇸",de:"🇩🇪",it:"🇮🇹",ar:"🇸🇦",pt:"🇵🇹",other:"📺"};
    const names = {fr:"Français",en:"English",es:"Español",de:"Deutsch",it:"Italiano",ar:"Arabe",pt:"Português",other:"Autres"};
    const entries = Object.entries(lang_counts).sort((a,b)=>b[1]-a[1]);
    if(!entries.length){ el.innerHTML = '<div class="fav-empty">aucune donnée</div>'; return; }
    const max = Math.max(...entries.map(e=>e[1]), 1);
    el.innerHTML = entries.map(([lang,n]) => `
        <div class="chart-row">
            <div class="chart-label">${flags[lang]||"🌍"} ${names[lang]||lang}</div>
            <div class="chart-bar"><div class="chart-fill" style="width:${Math.round(n/max*100)}%"></div></div>
            <div class="chart-count">${n}</div>
        </div>`).join("");
}

async function loadActivity() {
    try {
        const r = await apiFetch("/api/activity");
        const log = await r.json();
        const el = $("#activity-list");
        if (!log.length) { el.innerHTML = "aucune activité pour le moment"; return; }
        el.innerHTML = log.slice(0, 20).map(e =>
            `<div style="padding:6px 0;border-bottom:1px solid var(--border)">
                <span style="color:var(--text-primary)">${escapeHtml(e.action)}</span>
                ${e.details ? ' — ' + escapeHtml(e.details) : ''}
                <span style="float:right;color:var(--text-secondary)">${e.time}</span>
            </div>`
        ).join("");
    } catch(e) {
        if (e.message !== 'unauthenticated') console.error("Activity error:", e);
    }
}

async function loadManualChannels() {
    try {
        const r = await apiFetch("/api/manual-channels");
        const channels = await r.json();
        const list = $("#manual-channels-list");
        
        if (channels.length === 0) {
            list.innerHTML = '<div style="color:var(--text-secondary);text-align:center;padding:30px;grid-column:1/-1">Aucune source ajoutée</div>';
            return;
        }
        
        list.innerHTML = channels.map(ch => `
            <div class="manual-channel-item">
                <div class="manual-channel-info">
                    <div class="manual-channel-name">${escapeHtml(ch.name)}</div>
                    <div class="manual-channel-meta">ID: ${escapeHtml(ch.id)} · Ajoutée le ${escapeHtml(ch.added_at || 'N/A')}</div>
                </div>
                <button class="remove-btn" onclick="removeChannel('${escapeHtml(ch.id)}')">🗑️ Supprimer</button>
            </div>
        `).join('');
    } catch(e) {
        if (e.message !== 'unauthenticated') console.error("Load manual channels error:", e);
    }
}

async function removeChannel(id) {
    if (!confirm('Supprimer cette chaîne ?')) return;
    
    try {
        const r = await apiFetch('/api/remove-channel', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({id})
        });
        const d = await r.json();
        
        if (d.success) {
            showToast('✅ Chaîne supprimée');
            await loadManualChannels();
            await refreshStats();
        } else {
            showToast('❌ Erreur: ' + d.message, 'error');
        }
    } catch(e) {
        if (e.message !== 'unauthenticated') showToast('❌ Erreur: ' + e.message, 'error');
    }
}

async function refreshAll() {
    try {
        const r = await apiFetch("/api/refresh-cache");
        const d = await r.json();
        await refreshStats();
        showToast(`✅ Cache rafraîchi : ${d.dlstreams} dlstreams / ${d.vavoo} vavoo`);
    } catch(e) {
        if (e.message !== 'unauthenticated') showToast('❌ Erreur de rafraîchissement', 'error');
    }
}

let CURRENT = "dlstreams", ALL = {dlstreams:[], vavoo:[]};
let LANG_FILTER = "fr";

async function loadCatalog(src){
    const url = src==="vavoo" ? "/api/vavoo-channels" : `/api/channels?lang=${LANG_FILTER}`;
    try{
        const r = await apiFetch(url);
        ALL[src] = await r.json();
    }catch(e){
        ALL[src]=[];
    }
}

function render(){
    const q = $("#q").value.toLowerCase().trim();
    const words = q ? q.split(/\s+/) : [];
    const lang = LANG_FILTER === "all" ? null : LANG_FILTER;
    
    const items = (ALL[CURRENT]||[]).filter(c => {
        if (lang && c.lang !== lang) return false;
        return words.every(w => (c.name||"").toLowerCase().includes(w));
    }).slice(0, 300);
    
    const list = $("#list");
    if(!items.length){
        list.innerHTML = '<div style="color:var(--text-secondary);text-align:center;padding:30px;grid-column:1/-1">aucun résultat</div>';
        return;
    }
    list.innerHTML = items.map(c => {
        const encodedId = CURRENT==="vavoo" ? b64u(c.id) : c.id;
        const href = CURRENT==="vavoo"
            ? `${BASE}/vhls?v=${encodeURIComponent(encodedId)}`
            : `${BASE}/hls/${c.id}/index.m3u8`;
        const logo = c.logo ? `<img src="${escapeHtml(c.logo)}" style="width:28px;height:28px;border-radius:6px;object-fit:cover;background:#000" onerror="this.style.display='none'">` : "";
        const key = (CURRENT==="vavoo"?"vavoo:":"dlstreams:")+c.id;
        return `<a class="channel-item" href="${href}" target="_blank" title="${escapeHtml(c.name)}" data-play="${href}">
            ${logo}
            <div class="name">${escapeHtml(c.name)}</div>
            <div class="id">${CURRENT==="vavoo"?"vavoo":"#"+c.id}</div>
            <span class="fav-star ${isFav(key)?"active":""}" onclick="event.preventDefault();event.stopPropagation();toggleFavKey('${escapeHtml(key)}')">★</span>
        </a>`;
    }).join("");
}

function getFavs(){ try{ return JSON.parse(localStorage.getItem("dl_favs")||"[]"); }catch(e){ return []; } }
function saveFavs(f){ localStorage.setItem("dl_favs", JSON.stringify(f)); }
function isFav(key){ return getFavs().some(f=>f.key===key); }
function toggleFavKey(key){
    const src = key.split(":")[0];
    const id = key.slice(key.indexOf(":")+1);
    const ch = (ALL[src]||[]).find(c => String(c.id)===String(id));
    if(ch) toggleFav(ch, src);
}
function toggleFav(ch, src){
    const key = src+":"+ch.id;
    const favs = getFavs();
    const i = favs.findIndex(f=>f.key===key);
    if(i>=0){ favs.splice(i,1); showToast("Retiré des favoris"); }
    else { favs.push({key, src, id: ch.id, name: ch.name, logo: ch.logo||""}); showToast("⭐ Ajouté aux favoris"); }
    saveFavs(favs);
    renderFavs();
    render();
}
function renderFavs(){
    const el = $("#fav-list");
    if(!el) return;
    const q = ($("#fav-q").value||"").toLowerCase().trim();
    const favs = getFavs().filter(f=>!q || (f.name||"").toLowerCase().includes(q)).slice(0,100);
    if(!favs.length){
        el.innerHTML = '<div class="fav-empty">Aucun favori — va dans le <a href="#" onclick="navigateTo(\'catalog\');return false">Catalogue</a> et clique sur ★ pour épingler une chaîne</div>';
        return;
    }
    el.innerHTML = favs.map(f=>{
        const href = f.src==="vavoo" ? `${BASE}/vhls?v=${encodeURIComponent(b64u(f.id))}` : `${BASE}/hls/${f.id}/index.m3u8`;
        const logo = f.logo ? `<img src="${escapeHtml(f.logo)}" style="width:28px;height:28px;border-radius:6px;object-fit:cover;background:#000" onerror="this.style.display='none'">` : "";
        return `<a class="channel-item" href="${href}" data-play="${href}" title="${escapeHtml(f.name)}">
            ${logo}
            <div class="name">${escapeHtml(f.name)}</div>
            <div class="id">${f.src==="vavoo"?"vavoo":"#"+f.id}</div>
            <span class="fav-star active" onclick="event.preventDefault();event.stopPropagation();removeFav('${escapeHtml(f.key)}')">★</span>
        </a>`;
    }).join("");
}
function removeFav(key){
    saveFavs(getFavs().filter(f=>f.key!==key));
    showToast("Retiré des favoris");
    renderFavs();
    render();
}

function b64u(s){ return btoa(unescape(encodeURIComponent(s))).replace(/=+$/,"").replace(/\+/g,"-").replace(/\//g,"_"); }
function escapeHtml(s){ return (s||"").replace(/[&<>"']/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c])); }

let _hls = null;
function openPlayer(url, title){
    $("#player-title").textContent = title;
    const video = $("#player-frame");
    if (_hls) { _hls.destroy(); _hls = null; }
    if (window.Hls && Hls.isSupported()) {
        _hls = new Hls();
        _hls.loadSource(url);
        _hls.attachMedia(video);
        _hls.on(Hls.Events.MANIFEST_PARSED, () => video.play().catch(()=>{}));
    } else {
        video.src = url;
        video.play().catch(()=>{});
    }
    $("#player-modal").classList.add("active");
}
$("#player-close").addEventListener("click", ()=>{
    const video = $("#player-frame");
    video.pause();
    video.src = "";
    if (_hls) { _hls.destroy(); _hls = null; }
    $("#player-modal").classList.remove("active");
});
document.addEventListener("click", (e)=>{
    if(e.target.closest(".channel-item") && e.target.closest(".channel-item").dataset.play){
        e.preventDefault();
        const item = e.target.closest(".channel-item");
        const name = item.querySelector(".name").textContent;
        openPlayer(item.dataset.play, name);
    }
});

$("#add-source-btn").addEventListener("click", async ()=>{
    const url = $("#source-url").value.trim();
    if(!url){
        $("#add-source-result").innerHTML = '<div class="alert alert-error">Veuillez entrer une URL</div>';
        return;
    }
    $("#add-source-btn").disabled = true;
    $("#add-source-btn").textContent = "⏳ Scraping...";
    try{
        const r = await apiFetch(`/api/add-source?url=${encodeURIComponent(url)}`);
        const d = await r.json();
        if(d.success){
            $("#add-source-result").innerHTML = `<div class="alert alert-success">${d.message}</div>`;
            $("#source-url").value = "";
            showToast(d.message);
            await loadManualChannels();
            await refreshStats();
        }else{
            $("#add-source-result").innerHTML = `<div class="alert alert-error">${d.message}</div>`;
            showToast(d.message, 'error');
        }
    }catch(e){
        if (e.message !== 'unauthenticated') {
            $("#add-source-result").innerHTML = `<div class="alert alert-error">Erreur: ${e.message}</div>`;
            showToast('Erreur: ' + e.message, 'error');
        }
    }finally{
        $("#add-source-btn").disabled = false;
        $("#add-source-btn").textContent = "🔍 Scraper & Ajouter";
    }
});

$("#q").addEventListener("input", (()=>{let t;return()=>{clearTimeout(t);t=setTimeout(render,120);}})());
$("#fav-q").addEventListener("input", (()=>{let t;return()=>{clearTimeout(t);t=setTimeout(renderFavs,120);}})());
$("#lang-filter").addEventListener("change", (e) => {
    LANG_FILTER = e.target.value;
    loadCatalog(CURRENT);
    render();
});
document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",async ()=>{
    document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));
    b.classList.add("active");
    CURRENT = b.dataset.src;
    if(!ALL[CURRENT].length) await loadCatalog(CURRENT);
    render();
}));
document.querySelectorAll(".copy-btn").forEach(b=>b.addEventListener("click",()=>{
    const el = $("#"+b.dataset.copy);
    const txt = el.href || el.textContent;
    navigator.clipboard.writeText(txt).then(()=>{
        showToast('✅ URL copiée');
        const old = b.textContent;
        b.textContent = "✓ copié";
        setTimeout(()=>b.textContent=old,1200);
    });
}));

function initLinks(){
    const m = `${BASE}/manifest.json`;
    $("#manifest").href = m;
    $("#manifest").textContent = m;
    $("#vlc").textContent = `${BASE}/hls/121/index.m3u8`;
    $("#host").textContent = BASE;
}

async function boot(){
    await Promise.all([refreshStats(), loadCatalog("dlstreams")]);
    render();
    renderFavs();
    initLinks();
    loadActivity();
    setInterval(refreshStats, 30000);
}

// Vérifier la session au chargement
checkSession();
</script>
</body>
</html>
"""

CONFIGURE_HTML = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>dlstreams — configuration</title>
<style>
    :root{--primary:#6366f1;--secondary:#ec4899;--bg:#0f172a;--bg2:#1e293b;--card:#1e293b;--border:#334155;--text:#f1f5f9;--muted:#94a3b8;--ok:#10b981;--gradient:linear-gradient(135deg,#6366f1 0%,#ec4899 100%)}
    *{box-sizing:border-box}html,body{margin:0;padding:0;background:var(--bg);background-image:radial-gradient(circle at 20% 50%,rgba(99,102,241,0.15) 0%,transparent 50%),radial-gradient(circle at 80% 80%,rgba(236,72,153,0.15) 0%,transparent 50%);color:var(--text);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;min-height:100vh}
    header{padding:28px 24px 8px;display:flex;align-items:center;gap:14px}
    header .logo{width:40px;height:40px;border-radius:10px;background:var(--gradient);display:grid;place-items:center;font-weight:800;color:#0b0f1a;box-shadow:0 8px 24px rgba(99,102,241,.3)}
    header h1{margin:0;font-size:20px;letter-spacing:.3px;background:var(--gradient);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
    main{padding:24px;max-width:800px;margin:0 auto}
    .card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:24px;margin-bottom:20px;backdrop-filter:blur(8px)}
    .card h2{margin:0 0 16px;color:var(--muted);font-size:15px;text-transform:uppercase;letter-spacing:.1em}
    .lang-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-top:16px}
    .lang-btn{padding:16px;border:2px solid var(--border);border-radius:12px;background:var(--bg2);color:var(--text);cursor:pointer;text-align:left;transition:.2s;display:flex;align-items:center;gap:12px}
    .lang-btn:hover{border-color:var(--primary);transform:translateY(-2px)}
    .lang-btn.selected{border-color:var(--ok);background:rgba(16,185,129,.1)}
    .lang-flag{font-size:24px}.lang-name{font-weight:600}.lang-count{color:var(--muted);font-size:12px;margin-top:2px}
    .manifest-box{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:12px;margin-top:16px;word-break:break-all;font-family:ui-monospace,monospace;font-size:12px}
    .copy{display:inline-flex;align-items:center;gap:6px;margin-top:12px;padding:10px 16px;border:1px solid var(--border);border-radius:8px;background:rgba(255,255,255,.02);color:var(--muted);font-size:13px;cursor:pointer;transition:.15s}
    .copy:hover{color:var(--text);border-color:var(--primary)}
    .info{color:var(--muted);font-size:13px;margin-top:12px;line-height:1.6}
    .badge{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px;background:rgba(99,102,241,.15);color:var(--primary);margin-left:6px}
    a{color:var(--primary);text-decoration:none}a:hover{color:var(--secondary)}
</style>
</head>
<body>
<header><div class="logo">▶</div><div><h1>Configuration <span class="badge">langue</span></h1></div></header>
<main>
  <div class="card"><h2>🌍 Choisir votre langue</h2><p style="margin:0 0 12px;color:var(--muted)">Sélectionnez la langue des chaînes à afficher dans Stremio :</p>
    <div class="lang-grid" id="lang-grid">
      <button class="lang-btn" data-lang="all"><span class="lang-flag">🌍</span><div><div class="lang-name">Toutes langues</div><div class="lang-count">Affiche tout le catalogue</div></div></button>
      <button class="lang-btn selected" data-lang="fr"><span class="lang-flag">🇫🇷</span><div><div class="lang-name">Français</div><div class="lang-count">Chaînes FR uniquement</div></div></button>
      <button class="lang-btn" data-lang="en"><span class="lang-flag">🇬🇧</span><div><div class="lang-name">English</div><div class="lang-count">Chaînes anglaises</div></div></button>
      <button class="lang-btn" data-lang="es"><span class="lang-flag">🇪🇸</span><div><div class="lang-name">Español</div><div class="lang-count">Chaînes espagnoles</div></div></button>
      <button class="lang-btn" data-lang="de"><span class="lang-flag">🇩🇪</span><div><div class="lang-name">Deutsch</div><div class="lang-count">Chaînes allemandes</div></div></button>
      <button class="lang-btn" data-lang="it"><span class="lang-flag">🇮🇹</span><div><div class="lang-name">Italiano</div><div class="lang-count">Chaînes italiennes</div></div></button>
      <button class="lang-btn" data-lang="ar"><span class="lang-flag">🇸🇦</span><div><div class="lang-name">Arabe</div><div class="lang-count">Chaînes arabes</div></div></button>
      <button class="lang-btn" data-lang="pt"><span class="lang-flag">🇵🇹</span><div><div class="lang-name">Português</div><div class="lang-count">Chaînes portugaises</div></div></button>
    </div>
  </div>
  <div class="card"><h2>📥 Installer dans Stremio</h2><p style="margin:0 0 12px;color:var(--muted)">URL du manifest à copier dans Stremio (Addons → Install via URL) :</p>
    <div class="manifest-box" id="manifest-url">—</div>
    <button class="copy" id="copy-btn">📋 Copier l'URL</button>
    <div class="info"><strong>Comment faire :</strong><br>1. Choisissez votre langue ci-dessus<br>2. Copiez l'URL du manifest<br>3. Dans Stremio : Addons → Icône puzzle → "Install via URL"<br>4. Collez l'URL et validez<br><br><em>L'addon n'affichera QUE les chaînes de la langue sélectionnée.</em></div>
  </div>
  <div class="card"><h2>🔗 Liens rapides</h2><div style="display:grid;gap:8px">
    <div><a href="/dashboard">→ Dashboard</a> — Voir et tester les chaînes</div>
    <div><a href="/manifest.json">→ Manifest standard</a> — Toutes langues</div>
    <div><a href="/">→ Retour accueil</a></div>
  </div></div>
</main>
<script>
const BASE = location.origin;
const $ = s => document.querySelector(s);
let CURRENT_LANG = "fr";
document.querySelectorAll(".lang-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".lang-btn").forEach(b => b.classList.remove("selected"));
    btn.classList.add("selected");
    CURRENT_LANG = btn.dataset.lang;
    updateManifest();
  });
});
function updateManifest() {
  const url = CURRENT_LANG === "all" 
    ? `${BASE}/manifest.json`
    : `${BASE}/manifest.json?lang=${CURRENT_LANG}`;
  $("#manifest-url").textContent = url;
}
$("#copy-btn").addEventListener("click", () => {
  const url = $("#manifest-url").textContent;
  navigator.clipboard.writeText(url).then(() => {
    const old = $("#copy-btn").textContent;
    $("#copy-btn").textContent = "✓ Copié !";
    setTimeout(() => $("#copy-btn").textContent = old, 2000);
  });
});
updateManifest();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
