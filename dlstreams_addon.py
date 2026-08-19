#!/usr/bin/env python3
"""dlstreams -> Stremio : mini-addon + proxy autonome (stdlib pure, ZERO dependance).
dlstreams.st enveloppe le reseau DaddyLive. Chaque chaine = un id (watch.php?id=N).
Le flux reel est un m3u8 tokenise, MAIS deux pieges :
- la PLAYLIST exige les headers Referer/Origin de l'embed (sinon 403) ;
- les SEGMENTS sont du MPEG-TS servi avec un Content-Type mensonger `application/zstd`
(ffmpeg/hls.js s'y cassent les dents si on ne le corrige pas ; les octets, eux, sont du TS).
Ce script :
1. resout la chaine (dlstreams -> iframe DaddyLive -> m3u8 en base64) ;
2. sert un PROXY local qui injecte les headers sur les playlists et rebalise les
segments en `video/mp2t` -> n'importe quel player ouvre l'URL locale et ca joue, SANS MediaFlow ;
3. expose un ADDON STREMIO minimal (manifest + catalog + stream) par-dessus ;
4. expose un DASHBOARD web moderne pour visualiser les stats et parcourir le catalogue.
Lancer :    python3 dlstreams_addon.py            (port 8781 par defaut, override: PORT=... )
VLC/ffmpeg: http://127.0.0.1:8781/hls/121/index.m3u8
Stremio :   installer via  http://<ton-ip-LAN>:8781/manifest.json
Dashboard:  http://127.0.0.1:8781/
Rien d'autre a installer : Python 3.8+ suffit. Aucune cle, aucun compte.
"""
from __future__ import annotations
import base64
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8781"))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
SITE = "https://dlstreams.st"
_CH_TTL = 1800
_START_TIME = time.time()

def _get(url: str, referer: str = SITE + "/", extra: dict | None = None, timeout: int = 20) -> bytes:
    """GET brut avec User-Agent + Referer (et Origin si fourni). Renvoie le corps (bytes)."""
    headers = {"User-Agent": UA, "Referer": referer}
    if extra:
        headers.update(extra)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

# ---------------------------------------------------------------- annuaire (id -> nom)
class _LinkParser(HTMLParser):
    """Extrait les <a href=...watch.php?id=N ...>Nom</a> de la home dlstreams."""
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

def channels() -> list[dict]:
    """Annuaire dlstreams -> [{id, name}]. Cache 30 min. Dedoublonne par id (garde le 1er nom)."""
    now = time.time()
    if _ch_cache["list"] and now - _ch_cache["at"] < _CH_TTL:
        return _ch_cache["list"]
    try:
        html = _get(SITE + "/").decode("utf-8", "replace")
    except Exception:
        return _ch_cache["list"]
    p = _LinkParser()
    p.feed(html)
    seen: dict[str, str] = {}
    for idv, name in p.items:
        seen.setdefault(idv, name)
    out = [{"id": i, "name": n} for i, n in seen.items()]
    _ch_cache.update(at=now, list=out)
    return out

def search(query: str, limit: int = 40) -> list[dict]:
    q = query.lower().strip()
    hits = [c for c in channels() if q in c["name"].lower()]
    return hits[:limit]

# ---------------------------------------------------------------- resolution du flux (MULTI-PLAYER)
_PLAYER_PATHS = ("stream", "watch", "player", "plus", "hub", "cast", "casting")

def _txt(url: str, referer: str = SITE + "/") -> str:
    return _get(url, referer=referer).decode("utf-8", "replace")

def players(cid: str) -> list[tuple[str, str]]:
    """[(label, url_page_player)] dans l'ordre de watch.php (repli = chemins connus)."""
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
    """m3u8 d'une page embed : `atob('<b64>')` (daddy2) OU en clair/echappe \\/ (wideiptv...)."""
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
    """Page-player -> (m3u8, host_embed pour Referer/Origin). Suit l'iframe (+1 hop si CDN imbrique)."""
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
    """FAILOVER : (m3u8, host) du 1er player qui livre un flux. Leve si aucun."""
    for _label, url in players(cid):
        try:
            return resolve_player(url)
        except Exception:
            continue
    raise ValueError("aucun player ne resout")

def working_players(cid: str) -> list[tuple[int, str]]:
    """[(index, label)] des players qui resolvent REELLEMENT un flux (verifies en parallele)."""
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

# ---------------------------------------------------------------- Vavoo (protocole mediahubmx)
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
    """POST {base}/{prefixe}-{action}.json sur le 1er mirror qui repond. -> JSON ou None."""
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
    """Catalogue Vavoo d'un pays -> [{id, name, logo}]. Cache 6 h, pagine."""
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
        items += [{"id": x.get("url"), "name": x.get("name") or "", "logo": x.get("logo") or ""}
                  for x in batch if x.get("url")]
        cursor, pages = d.get("nextCursor"), pages + 1
    if items:
        _vavoo_cache.update(at=time.time(), list=items)
    return _vavoo_cache["list"]

def vavoo_resolve(vurl: str) -> str:
    """URL de flux reelle pour une chaine Vavoo (resolution PARESSEUSE, URLs ephemeres)."""
    if not vurl:
        return ""
    d = _vavoo_post("resolve", {"language": "fr", "region": "FR", "url": str(vurl), "clientVersion": "3.0.2"})
    if isinstance(d, list) and d:
        return d[0].get("url") or d[0].get("streamUrl") or ""
    return ""

# ---------------------------------------------------------------- proxy HLS (headers par source)
def _b64u(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")

def _unb64u(s: str) -> str:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()

def _proxy_get(url: str, hdr: dict, timeout: int = 25) -> bytes:
    """GET une URL avec un jeu de headers arbitraire."""
    headers = {"User-Agent": UA}
    headers.update(hdr or {})
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def _rewrite_playlist(text: str, playlist_url: str, hdr_enc: str, self_base: str) -> str:
    """Reecrit une playlist HLS : chaque URL passe par NOTRE proxy."""
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

# ---------------------------------------------------------------- helpers dashboard
def _stats() -> dict:
    return {
        "status": "ok",
        "uptime": int(time.time() - _START_TIME),
        "version": "1.2.0",
        "port": PORT,
        "dlstreams": {
            "count": len(_ch_cache.get("list") or []),
            "age_seconds": int(time.time() - _ch_cache.get("at", 0)) if _ch_cache.get("at") else None,
        },
        "vavoo": {
            "count": len(_vavoo_cache.get("list") or []),
            "age_seconds": int(time.time() - _vavoo_cache.get("at", 0)) if _vavoo_cache.get("at") else None,
        },
    }

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, code: int, body: bytes, ctype: str, cache: bool = False):
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

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        u = urllib.parse.urlsplit(self.path)
        path = u.path
        qs = urllib.parse.parse_qs(u.query)
        try:
            # ---- dashboard & API ----
if path == "/dashboard" or path == "/dashboard.html":
    return self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8", True)

            if path == "/api/stats":
                return self._send(200, json.dumps(_stats()).encode(), "application/json")

            if path == "/api/channels":
                return self._send(200, json.dumps(channels()).encode(), "application/json", True)

            if path == "/api/vavoo-channels":
                return self._send(200, json.dumps(vavoo_channels()).encode(), "application/json", True)

            # ---- addon Stremio ----
            if path == "/manifest.json":
                return self._send(200, json.dumps(self._manifest()).encode(), "application/json", True)

            if path.startswith("/catalog/tv/"):
                extra = path[len("/catalog/tv/"):].removesuffix(".json")
                catid = extra.split("/", 1)[0]
                params = {}
                if "/" in extra:
                    for kv in extra.split("/", 1)[1].split("&"):
                        if "=" in kv:
                            k, v = kv.split("=", 1)
                            params[k] = urllib.parse.unquote_plus(v)
                chans = vavoo_channels() if catid == "vavoo" else channels()
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

            # ---- proxy HLS ----
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

    # ---- helpers addon ----
    def _manifest(self) -> dict:
        _extra = [{"name": "search", "isRequired": False}, {"name": "skip", "isRequired": False}]
        return {
            "id": "st.dlstreams.proxy",
            "version": "1.2.0",
            "name": "dlstreams + Vavoo",
            "description": "Chaines live dlstreams (DaddyLive) + Vavoo, proxifiees (headers + content-type "
                           "corriges). Dashboard web integre. Sans MediaFlow.",
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
    print(f"  Dashboard: http://127.0.0.1:{PORT}/")
    print(f"  Stremio  : http://<ton-ip-LAN>:{PORT}/manifest.json")
    print(f"  VLC/mpv  : http://127.0.0.1:{PORT}/hls/121/index.m3u8")
    try:
        n = len(channels())
        print(f"  annuaire : {n} chaines chargees")
    except Exception as e:
        print(f"  annuaire : erreur de chargement ({e})")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

# ---------------------------------------------------------------- DASHBOARD HTML (inline, zero dependance)
DASHBOARD_HTML = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>dlstreams — dashboard</title>
<style>
  :root{
    --bg:#0b0f1a; --bg2:#111827; --card:#151b2b; --border:#1f2937;
    --text:#e5e7eb; --muted:#94a3b8; --accent:#60a5fa; --accent2:#a78bfa;
    --ok:#34d399; --warn:#fbbf24; --err:#f87171;
  }
  *{box-sizing:border-box}
  html,body{margin:0;padding:0;background:
    radial-gradient(1200px 600px at 10% -10%, #1e293b 0%, transparent 60%),
    radial-gradient(900px 500px at 110% 10%, #312e81 0%, transparent 60%),
    var(--bg);
    color:var(--text);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;min-height:100vh}
  header{padding:28px 24px 8px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
  header .logo{width:40px;height:40px;border-radius:10px;
    background:linear-gradient(135deg,var(--accent),var(--accent2));
    display:grid;place-items:center;font-weight:800;color:#0b0f1a;box-shadow:0 8px 24px rgba(96,165,250,.3)}
  header h1{margin:0;font-size:20px;letter-spacing:.3px}
  header .sub{color:var(--muted);font-size:12px}
  .status{margin-left:auto;display:flex;align-items:center;gap:8px;
    padding:6px 12px;border:1px solid var(--border);border-radius:999px;background:rgba(255,255,255,.02)}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--muted);box-shadow:0 0 0 0 rgba(52,211,153,.6)}
  .dot.ok{background:var(--ok);animation:pulse 2s infinite}
  .dot.err{background:var(--err)}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(52,211,153,.6)}70%{box-shadow:0 0 0 10px rgba(52,211,153,0)}100%{box-shadow:0 0 0 0 rgba(52,211,153,0)}}
  main{padding:16px 24px 60px;max-width:1200px;margin:0 auto}
  .grid{display:grid;gap:14px}
  .cards{grid-template-columns:repeat(auto-fit,minmax(200px,1fr))}
  .card{background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01));
    border:1px solid var(--border);border-radius:14px;padding:16px 18px;backdrop-filter:blur(8px)}
  .card .label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em}
  .card .val{font-size:26px;font-weight:700;margin-top:6px;letter-spacing:.3px}
  .card .hint{color:var(--muted);font-size:12px;margin-top:4px}
  section{margin-top:22px}
  section h2{font-size:15px;margin:0 0 10px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em}
  .access{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}
  .access .card a{color:var(--accent);text-decoration:none;word-break:break-all}
  .access .card a:hover{color:var(--accent2)}
  .copy{display:inline-flex;align-items:center;gap:6px;margin-top:8px;padding:6px 10px;
    border:1px solid var(--border);border-radius:8px;background:rgba(255,255,255,.02);
    color:var(--muted);font-size:12px;cursor:pointer;transition:.15s}
  .copy:hover{color:var(--text);border-color:var(--accent)}
  .search{position:sticky;top:0;z-index:5;background:rgba(11,15,26,.85);backdrop-filter:blur(10px);
    padding:10px 0;display:flex;gap:10px;align-items:center}
  .search input{flex:1;background:var(--card);border:1px solid var(--border);color:var(--text);
    padding:10px 14px;border-radius:10px;font-size:14px;outline:none;transition:.15s}
  .search input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(96,165,250,.15)}
  .tabs{display:flex;gap:6px}
  .tab{padding:8px 14px;border-radius:8px;border:1px solid var(--border);background:transparent;
    color:var(--muted);cursor:pointer;font-size:13px;transition:.15s}
  .tab.active{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#0b0f1a;border-color:transparent;font-weight:600}
  .list{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px;margin-top:10px}
  .item{display:flex;align-items:center;gap:10px;padding:10px;border:1px solid var(--border);
    border-radius:10px;background:var(--card);cursor:pointer;transition:.15s;text-decoration:none;color:var(--text)}
  .item:hover{border-color:var(--accent);transform:translateY(-1px)}
  .item .name{flex:1;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .item .id{color:var(--muted);font-size:11px;font-family:ui-monospace,monospace}
  .empty{color:var(--muted);text-align:center;padding:30px}
  footer{margin-top:40px;color:var(--muted);font-size:12px;text-align:center}
  .badge{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px;
    background:rgba(96,165,250,.15);color:var(--accent);margin-left:6px}
  @media (max-width:600px){header{padding:18px 14px}main{padding:10px 14px 40px}.card .val{font-size:22px}}
</style>
</head>
<body>
<header>
  <div class="logo">▶</div>
  <div>
    <h1>dlstreams <span class="badge">addon + proxy</span></h1>
    <div class="sub">Chaînes live DaddyLive + Vavoo — servi sur le port 8781</div>
  </div>
  <div class="status"><span id="dot" class="dot"></span><span id="stxt">vérification…</span></div>
</header>

<main>
  <section>
    <div class="grid cards" id="cards">
      <div class="card"><div class="label">Chaînes dlstreams</div><div class="val" id="c-dl">—</div><div class="hint" id="c-dl-h">cache</div></div>
      <div class="card"><div class="label">Chaînes Vavoo</div><div class="val" id="c-vv">—</div><div class="hint" id="c-vv-h">catalogue FR</div></div>
      <div class="card"><div class="label">Uptime</div><div class="val" id="c-up">—</div><div class="hint">depuis démarrage</div></div>
      <div class="card"><div class="label">Version</div><div class="val" id="c-v">—</div><div class="hint">addon Stremio</div></div>
    </div>
  </section>

  <section>
    <h2>Accès rapide</h2>
    <div class="grid access">
      <div class="card">
        <div class="label">Stremio — installer l'addon</div>
        <div style="margin-top:6px">Addons → « Install via URL »</div>
        <a id="manifest" href="#">—</a>
        <div><button class="copy" data-copy="manifest">📋 copier l'URL</button></div>
      </div>
      <div class="card">
        <div class="label">VLC / mpv / ffmpeg — lecture directe</div>
        <div style="margin-top:6px">Ouvre un flux par son id :</div>
        <code id="vlc" style="color:var(--accent);word-break:break-all;font-size:12px">—</code>
        <div><button class="copy" data-copy="vlc">📋 copier</button></div>
      </div>
      <div class="card">
        <div class="label">API</div>
        <div style="margin-top:6px;font-size:12px;color:var(--muted)">
          <div><a href="/manifest.json" style="color:var(--accent)">/manifest.json</a> — Stremio</div>
          <div><a href="/api/stats" style="color:var(--accent)">/api/stats</a> — statut serveur</div>
          <div><a href="/api/channels" style="color:var(--accent)">/api/channels</a> — annuaire dlstreams</div>
          <div><a href="/api/vavoo-channels" style="color:var(--accent)">/api/vavoo-channels</a> — catalogue Vavoo FR</div>
        </div>
      </div>
    </div>
  </section>

  <section>
    <h2>Catalogue</h2>
    <div class="search">
      <input id="q" type="search" placeholder="Rechercher une chaîne (ex : beIN, Canal+, RMC Sport…)">
      <div class="tabs">
        <button class="tab active" data-src="dlstreams">dlstreams</button>
        <button class="tab" data-src="vavoo">Vavoo</button>
      </div>
    </div>
    <div class="list" id="list"><div class="empty">chargement…</div></div>
  </section>

  <footer>
    dlstreams addon+proxy · Python stdlib pure · zéro dépendance · <span id="host"></span>
  </footer>
</main>

<script>
const BASE = location.origin;
const $ = s => document.querySelector(s);
const fmtDur = s => {
  if (s==null) return "—";
  const d=Math.floor(s/86400), h=Math.floor(s%86400/3600), m=Math.floor(s%3600/60);
  return (d?d+"j ":"")+(h?h+"h ":"")+(m+"m");
};
const fmtAge = s => s==null ? "pas encore chargé" : (s<60?s+"s":Math.floor(s/60)+"min");

async function refreshStats(){
  try{
    const r = await fetch("/api/stats"); const d = await r.json();
    $("#c-dl").textContent = d.dlstreams.count;
    $("#c-dl-h").textContent = "cache : " + fmtAge(d.dlstreams.age_seconds);
    $("#c-vv").textContent = d.vavoo.count;
    $("#c-vv-h").textContent = "cache : " + fmtAge(d.vavoo.age_seconds);
    $("#c-up").textContent = fmtDur(d.uptime);
    $("#c-v").textContent = "v" + d.version;
    $("#dot").className = "dot ok"; $("#stxt").textContent = "en ligne";
  }catch(e){
    $("#dot").className = "dot err"; $("#stxt").textContent = "hors ligne";
  }
}

let CURRENT = "dlstreams", ALL = {dlstreams:[], vavoo:[]};
async function loadCatalog(src){
  const url = src==="vavoo" ? "/api/vavoo-channels" : "/api/channels";
  try{ const r = await fetch(url); ALL[src] = await r.json(); }catch(e){ ALL[src]=[]; }
}
function render(){
  const q = $("#q").value.toLowerCase().trim();
  const words = q ? q.split(/\s+/) : [];
  const items = (ALL[CURRENT]||[]).filter(c =>
    words.every(w => (c.name||"").toLowerCase().includes(w))
  ).slice(0, 300);
  const list = $("#list");
  if(!items.length){ list.innerHTML = '<div class="empty">aucun résultat</div>'; return; }
  list.innerHTML = items.map(c => {
    const encodedId = CURRENT==="vavoo" ? b64u(c.id) : c.id;
    const href = CURRENT==="vavoo"
      ? `${BASE}/vhls?v=${encodeURIComponent(encodedId)}`
      : `${BASE}/hls/${c.id}/index.m3u8`;
    const logo = c.logo ? `<img src="${c.logo}" style="width:28px;height:28px;border-radius:6px;object-fit:cover;background:#000" onerror="this.style.display='none'">` : "";
    return `<a class="item" href="${href}" target="_blank" title="${escapeHtml(c.name)}">
      ${logo}
      <div class="name">${escapeHtml(c.name)}</div>
      <div class="id">${CURRENT==="vavoo"?"vavoo":"#"+c.id}</div>
    </a>`;
  }).join("");
}
function b64u(s){ return btoa(unescape(encodeURIComponent(s))).replace(/=+$/,"").replace(/\+/g,"-").replace(/\//g,"_"); }
function escapeHtml(s){ return (s||"").replace(/[&<>"']/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c])); }

$("#q").addEventListener("input", (()=>{let t;return()=>{clearTimeout(t);t=setTimeout(render,120);}})());
document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",async ()=>{
  document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));
  b.classList.add("active"); CURRENT = b.dataset.src;
  if(!ALL[CURRENT].length) await loadCatalog(CURRENT);
  render();
}));
document.querySelectorAll(".copy").forEach(b=>b.addEventListener("click",()=>{
  const el = $("#"+b.dataset.copy); const txt = el.href || el.textContent;
  navigator.clipboard.writeText(txt).then(()=>{
    const old = b.textContent; b.textContent = "✓ copié"; setTimeout(()=>b.textContent=old,1200);
  });
}));

(function initLinks(){
  const m = `${BASE}/manifest.json`;
  $("#manifest").href = m; $("#manifest").textContent = m;
  $("#vlc").textContent = `${BASE}/hls/121/index.m3u8`;
  $("#host").textContent = BASE;
})();

(async function boot(){
  await Promise.all([refreshStats(), loadCatalog("dlstreams")]);
  render();
  setInterval(refreshStats, 30000);
})();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
