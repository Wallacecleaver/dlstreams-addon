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
import struct
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8781"))
_VERSION = "1.8.0"

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
_hist: list[list[int]] = []      # [minute_unix, nb_requetes] -> sparkline (1h)
_request_log: list[dict] = []    # journal des requetes (buffer 300)
_chan_plays: dict[str, int] = {}  # cle "src:id" -> nb de lectures de flux
_recent_plays: list[dict] = []    # dernieres lectures (buffer 40)
_HIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_hist.json")
_HIST_KEEP_MIN = 7 * 24 * 60      # minutes conservees dans l'historique (7 jours)

# --- sessions dashboard : jeton opaque valide cote serveur, stocke en memoire
# (pas de JWT/dependance -- juste un dict token -> heure d'emission). Remplace
# l'ancien systeme qui ne verifiait le mot de passe qu'une fois sans jamais
# proteger les endpoints /api/* ensuite. Persistees sur disque pour survivre
# aux redemarrages (Render recycle souvent l'instance, ce qui invalidait la
# session et forçait a se reconnecter). ---
_sessions: dict[str, float] = {}
_SESSION_TTL = 24 * 3600
_SESSION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_sessions.json")
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

def _genre_for(name: str) -> list[str]:
    n = name.lower()
    if any(k in n for k in ["sport", "foot", "tennis", "racing", "formula", "f1 ", "golf", "cycl", "beinsport", "eurosport", "rmc", "canal+ sport", "ufc", "boxe", "mma"]):
        return ["Sports"]
    if any(k in n for k in ["news", "info", "bfm", "cnews", "france info", "cnn", "bbc", "sky news", "al jazeera", "rt "]):
        return ["Actualités"]
    if any(k in n for k in ["cinema", "cinéma", "film", "séries", "series", "family", "kids", "gulli", "cartoon", "plus"]):
        return ["Films & Séries"]
    if any(k in n for k in ["musique", "music", "mtv", "radio", "clip"]):
        return ["Musique"]
    if any(k in n for k in ["découverte", "decouverte", "documentaire", "voyage", "histoire", "geo"]):
        return ["Documentaire"]
    return ["Télévision"]

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

def _sessions_load():
    """Recharge les sessions depuis le disque, en purgeant celles expirees."""
    global _sessions
    try:
        if os.path.exists(_SESSION_FILE):
            with open(_SESSION_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            now = time.time()
            _sessions = {k: float(v) for k, v in data.items()
                         if isinstance(v, (int, float)) and (now - float(v)) < _SESSION_TTL}
    except Exception:
        pass

def _sessions_save():
    """Persiste les sessions sur disque (appelee a la connexion/deconnexion)."""
    try:
        with open(_SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(_sessions, f)
    except Exception:
        pass

def _hist_load():
    """Charge l'historique de trafic persiste (7 jours) au demarrage."""
    global _hist
    try:
        if os.path.exists(_HIST_FILE):
            with open(_HIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            keep_from = time.time() // 60 - _HIST_KEEP_MIN
            _hist = [[int(m), int(c)] for m, c in data
                     if isinstance(m, (int, float)) and isinstance(c, (int, float))
                     and m >= keep_from]
    except Exception:
        pass

def _hist_save():
    """Persiste l'historique sur disque (appele une fois par minute)."""
    try:
        with open(_HIST_FILE, "w", encoding="utf-8") as f:
            json.dump(_hist, f)
    except Exception:
        pass

def _track_play(src: str, cid: str):
    key = f"{src}:{cid}"
    with _stats_lock:
        _chan_plays[key] = _chan_plays.get(key, 0) + 1
        _recent_plays.append({"src": src, "cid": cid, "t": time.time()})
        if len(_recent_plays) > 40:
            del _recent_plays[:len(_recent_plays) - 40]

def _name_for(src: str, cid: str) -> str:
    if src == "vavoo":
        ch = next((x for x in _vavoo_cache.get("list", []) if x.get("id") == cid), None)
    else:
        ch = next((x for x in _ch_cache.get("list", []) if str(x.get("id")) == str(cid)), None)
    return (ch.get("name") or cid) if ch else cid

def _top_channels(n: int = 30) -> list[dict]:
    with _stats_lock:
        items = sorted(_chan_plays.items(), key=lambda kv: -kv[1])[:n]
    out = []
    for key, plays in items:
        src, _, cid = key.partition(":")
        out.append({"key": key, "src": src, "id": cid, "name": _name_for(src, cid), "plays": plays})
    return out

def _recent_plays_list(n: int = 15) -> list[dict]:
    with _stats_lock:
        items = list(reversed(_recent_plays))[:n]
    out = []
    for p in items:
        key = f"{p['src']}:{p['cid']}"
        out.append({"key": key, "src": p["src"], "id": p["cid"],
                    "name": _name_for(p["src"], p["cid"]), "t": p["t"]})
    return out

def _plays_map() -> dict[str, int]:
    with _stats_lock:
        return dict(_chan_plays)

def _system_info() -> dict:
    import shutil
    import sys
    usage = None
    try:
        u = shutil.disk_usage(os.getcwd())
        usage = {"total": u.total, "used": u.used, "free": u.free}
    except Exception:
        pass
    mem: dict = {}
    try:
        import psutil
        vm = psutil.virtual_memory()
        proc = psutil.Process(os.getpid())
        mem = {"total": vm.total, "used": vm.used, "percent": vm.percent,
               "rss": proc.memory_info().rss, "cpu": proc.cpu_percent(interval=0.2)}
    except Exception:
        try:
            import resource
            mem = {"rss": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}
        except Exception:
            pass
    def _age(cache) -> int | None:
        at = cache.get("at") or 0
        return int(time.time() - at) if at else None
    return {
        "version": _VERSION,
        "python": sys.version.split()[0],
        "port": PORT,
        "pid": os.getpid(),
        "platform": sys.platform,
        "cpus": os.cpu_count() or 0,
        "uptime": int(time.time() - _START_TIME),
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(_START_TIME)),
        "disk": usage,
        "memory": mem,
        "cache": {
            "dlstreams": {"count": len(_ch_cache.get("list") or []), "age_seconds": _age(_ch_cache)},
            "vavoo": {"count": len(_vavoo_cache.get("list") or []), "age_seconds": _age(_vavoo_cache)},
        },
        "channels_total": len(channels()),
    }

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
        "version": _VERSION,
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
        "history": [list(pair) for pair in _hist],
        "daily_totals": _daily_totals(),
        "top_channels": _top_channels(),
        "recent_plays": _recent_plays_list(),
    }

def _daily_totals() -> list[dict]:
    days: dict[str, int] = {}
    now = time.time()
    with _stats_lock:
        for minute, count in _hist:
            if now - minute > 7 * 86400:
                continue
            day = time.strftime("%d/%m", time.localtime(minute))
            days[day] = days.get(day, 0) + count
    return [{"date": d, "total": days[d]} for d in sorted(days)]

# --- Posters generes (sans dependance, pur Python) ---
# Tuile paysage 320x180 dans l'univers graphique de l'addon (fond sombre
# + diagonale indigo/rose) avec le nom de la chaine. Utilise comme poster
# de secours quand une chaine n'a pas de logo reel (dlstreams n'en fournit
# aucun). Encodage PNG minimal (RGB, filtre 0) + police bitmap 5x7.
_FONT5X7 = [
    0x00,0x00,0x00,0x00,0x00,0x00,0x00, 0x04,0x04,0x04,0x04,0x04,0x00,0x04,
    0x0A,0x0A,0x0A,0x00,0x00,0x00,0x00, 0x0A,0x0A,0x1F,0x0A,0x1F,0x0A,0x0A,
    0x04,0x1E,0x05,0x0E,0x14,0x0F,0x04, 0x19,0x19,0x02,0x04,0x08,0x13,0x13,
    0x0C,0x12,0x12,0x0C,0x12,0x12,0x0C, 0x06,0x06,0x02,0x04,0x00,0x00,0x00,
    0x02,0x04,0x08,0x08,0x08,0x04,0x02, 0x08,0x04,0x02,0x02,0x02,0x04,0x08,
    0x00,0x04,0x15,0x0E,0x15,0x04,0x00, 0x00,0x04,0x04,0x1F,0x04,0x04,0x00,
    0x00,0x00,0x00,0x00,0x00,0x06,0x06, 0x00,0x00,0x00,0x1F,0x00,0x00,0x00,
    0x00,0x00,0x00,0x00,0x00,0x06,0x00, 0x01,0x02,0x02,0x04,0x08,0x08,0x10,
    0x0E,0x11,0x13,0x15,0x19,0x11,0x0E, 0x04,0x0C,0x04,0x04,0x04,0x04,0x0E,
    0x0E,0x11,0x10,0x08,0x04,0x02,0x1F, 0x1F,0x08,0x04,0x08,0x10,0x11,0x0E,
    0x08,0x0C,0x0A,0x09,0x1F,0x08,0x08, 0x1F,0x01,0x0F,0x10,0x10,0x11,0x0E,
    0x0E,0x01,0x01,0x0F,0x11,0x11,0x0E, 0x1F,0x10,0x08,0x04,0x04,0x04,0x04,
    0x0E,0x11,0x11,0x0E,0x11,0x11,0x0E, 0x0E,0x11,0x11,0x1E,0x10,0x10,0x0E,
    0x00,0x06,0x06,0x00,0x06,0x06,0x00, 0x00,0x06,0x06,0x00,0x06,0x06,0x02,
    0x02,0x04,0x08,0x10,0x08,0x04,0x02, 0x00,0x00,0x1F,0x00,0x1F,0x00,0x00,
    0x08,0x04,0x02,0x01,0x02,0x04,0x08, 0x0E,0x11,0x10,0x08,0x04,0x00,0x04,
    0x0E,0x11,0x10,0x16,0x15,0x15,0x0E, 0x0E,0x11,0x11,0x1F,0x11,0x11,0x11,
    0x0F,0x11,0x11,0x0F,0x11,0x11,0x0F, 0x0E,0x11,0x01,0x01,0x01,0x11,0x0E,
    0x0F,0x11,0x11,0x11,0x11,0x11,0x0F, 0x1F,0x01,0x01,0x0F,0x01,0x01,0x1F,
    0x1F,0x01,0x01,0x0F,0x01,0x01,0x01, 0x0E,0x11,0x01,0x1D,0x11,0x11,0x0E,
    0x11,0x11,0x11,0x1F,0x11,0x11,0x11, 0x0E,0x04,0x04,0x04,0x04,0x04,0x0E,
    0x18,0x08,0x08,0x08,0x08,0x09,0x06, 0x11,0x09,0x05,0x03,0x05,0x09,0x11,
    0x01,0x01,0x01,0x01,0x01,0x01,0x1F, 0x11,0x1B,0x15,0x11,0x11,0x11,0x11,
    0x11,0x13,0x15,0x19,0x11,0x11,0x11, 0x0E,0x11,0x11,0x11,0x11,0x11,0x0E,
    0x0F,0x11,0x11,0x0F,0x01,0x01,0x01, 0x0E,0x11,0x11,0x11,0x15,0x09,0x16,
    0x0F,0x11,0x11,0x0F,0x05,0x09,0x11, 0x0E,0x11,0x01,0x0E,0x10,0x11,0x0E,
    0x1F,0x04,0x04,0x04,0x04,0x04,0x04, 0x11,0x11,0x11,0x11,0x11,0x11,0x0E,
    0x11,0x11,0x11,0x11,0x11,0x0A,0x04, 0x11,0x11,0x11,0x15,0x15,0x1B,0x11,
    0x11,0x11,0x0A,0x04,0x0A,0x11,0x11, 0x11,0x11,0x0A,0x04,0x04,0x04,0x04,
    0x1F,0x10,0x08,0x04,0x02,0x01,0x1F, 0x0E,0x02,0x02,0x02,0x02,0x02,0x0E,
    0x10,0x08,0x08,0x04,0x02,0x02,0x01, 0x0E,0x08,0x08,0x08,0x08,0x08,0x0E,
    0x04,0x0A,0x11,0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00,0x00,0x00,0x1F,
    0x04,0x02,0x00,0x00,0x00,0x00,0x00, 0x00,0x00,0x0E,0x10,0x1E,0x11,0x1E,
    0x01,0x01,0x0D,0x13,0x11,0x11,0x0F, 0x00,0x00,0x0E,0x01,0x01,0x11,0x0E,
    0x10,0x10,0x1C,0x12,0x11,0x11,0x1E, 0x00,0x00,0x0E,0x11,0x1F,0x01,0x0E,
    0x0C,0x12,0x02,0x0F,0x02,0x02,0x02, 0x00,0x1E,0x11,0x11,0x1E,0x10,0x0E,
    0x01,0x01,0x0D,0x13,0x11,0x11,0x11, 0x04,0x00,0x0C,0x04,0x04,0x04,0x0E,
    0x08,0x00,0x18,0x08,0x08,0x08,0x06, 0x01,0x01,0x09,0x05,0x03,0x05,0x09,
    0x0C,0x04,0x04,0x04,0x04,0x04,0x0E, 0x00,0x00,0x0B,0x15,0x15,0x11,0x11,
    0x00,0x00,0x0D,0x13,0x11,0x11,0x11, 0x00,0x00,0x0E,0x11,0x11,0x11,0x0E,
    0x00,0x00,0x0F,0x11,0x11,0x0F,0x01, 0x00,0x00,0x1E,0x11,0x11,0x1E,0x10,
    0x00,0x00,0x0D,0x13,0x01,0x01,0x01, 0x00,0x00,0x0E,0x01,0x0E,0x10,0x0E,
    0x02,0x02,0x0F,0x02,0x02,0x12,0x0C, 0x00,0x00,0x11,0x11,0x11,0x13,0x0D,
    0x00,0x00,0x11,0x11,0x11,0x0A,0x04, 0x00,0x00,0x11,0x11,0x15,0x15,0x0A,
    0x00,0x00,0x11,0x0A,0x04,0x0A,0x11, 0x00,0x00,0x11,0x11,0x1E,0x10,0x0E,
    0x00,0x00,0x1F,0x08,0x04,0x02,0x1F, 0x02,0x04,0x04,0x08,0x04,0x04,0x02,
    0x04,0x04,0x04,0x00,0x04,0x04,0x04, 0x08,0x04,0x04,0x02,0x04,0x04,0x08,
    0x00,0x00,0x06,0x09,0x06,0x00,0x00,
]

def _png_chunk(tag: bytes, data: bytes) -> bytes:
    c = tag + data
    return struct.pack(">I", len(data)) + c + struct.pack(">I", zlib.crc32(c) & 0xffffffff)

def _png_encode(w: int, h: int, rgb: bytes) -> bytes:
    raw = bytearray()
    stride = w * 3
    for y in range(h):
        raw.append(0)
        raw += rgb[y * stride:(y + 1) * stride]
    ihdr = struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)
    return (b"\x89PNG\r\n\x1a\n"
            + _png_chunk(b"IHDR", ihdr)
            + _png_chunk(b"IDAT", zlib.compress(bytes(raw), 6))
            + _png_chunk(b"IEND", b""))

def _text_width(text: str, scale: int) -> int:
    return len(text) * 6 * scale

def _draw_text(buf: bytearray, w: int, text: str, x: int, y: int, scale: int, color: tuple[int, int, int]):
    for ch in text:
        idx = ord(ch) - 32
        if idx < 0 or idx >= len(_FONT5X7) // 7:
            idx = 0
        glyph = _FONT5X7[idx * 7:(idx + 1) * 7]
        for row in range(7):
            bits = glyph[row]
            for col in range(5):
                if bits & (1 << (4 - col)):
                    for dy in range(scale):
                        for dx in range(scale):
                            px, py = x + col * scale + dx, y + row * scale + dy
                            if 0 <= px < w and 0 <= py < len(buf) // (w * 3):
                                o = (py * w + px) * 3
                                buf[o:o + 3] = bytes(color)
        x += 6 * scale

def _fill_rect(buf: bytearray, w: int, x0: int, y0: int, x1: int, y1: int, color: tuple[int, int, int]):
    for yy in range(max(0, y0), min(y1, len(buf) // (w * 3))):
        for xx in range(max(0, x0), min(x1, w)):
            o = (yy * w + xx) * 3
            buf[o:o + 3] = bytes(color)

def _poster_png(name: str) -> bytes:
    W, H = 320, 180
    buf = bytearray(W * H * 3)
    c1, c2 = (15, 23, 42), (30, 27, 75)   # #0f172a -> #1e1b4b
    accent = (99, 102, 241)               # indigo #6366f1
    pink = (236, 72, 153)                 # rose #ec4899
    for y in range(H):
        t = y / (H - 1)
        for x in range(W):
            o = (y * W + x) * 3
            buf[o:o + 3] = bytes((int(c1[0] + (c2[0] - c1[0]) * t),
                                  int(c1[1] + (c2[1] - c1[1]) * t),
                                  int(c1[2] + (c2[2] - c1[2]) * t)))
    _fill_rect(buf, W, -80, 0, W, 40, (40, 44, 90))
    _fill_rect(buf, W, 0, H - 46, W, H - 26, (30, 33, 78))
    for x in range(W):
        prog = (x + (H - 26)) % (W + 260) / (W + 260)
        if 0 <= prog <= 1:
            col = (int(accent[0] + (pink[0] - accent[0]) * prog),
                   int(accent[1] + (pink[1] - accent[1]) * prog),
                   int(accent[2] + (pink[2] - accent[2]) * prog))
            _fill_rect(buf, W, x, H - 26, x + 1, H - 20, col)
    title = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().upper() or "TV"
    scale = 4
    max_w = W - 32
    # decoupage en lignes qui tiennent
    lines, cur = [], ""
    for word in title.split():
        trial = (cur + " " + word).strip()
        if _text_width(trial, scale) <= max_w:
            cur = trial
        else:
            if cur: lines.append(cur)
            cur = word
    if cur: lines.append(cur)
    if len(lines) > 2:
        lines = lines[:2]
    # echelle reduite si ca ne rentre toujours pas
    while any(_text_width(l, scale) > max_w for l in lines) and scale > 2:
        scale -= 1
    total_h = len(lines) * 7 * scale + (len(lines) - 1) * scale
    ty = (H - total_h) // 2 - 2
    for line in lines:
        tw = _text_width(line, scale)
        _draw_text(buf, W, line, (W - tw) // 2, ty, scale, (241, 245, 249))
        ty += 8 * scale
    # pastille LIVE
    pill_w, pill_h, pill_x, pill_y = 56, 20, 14, 12
    for y in range(pill_y, pill_y + pill_h):
        t = (y - pill_y) / pill_h
        for x in range(pill_x, pill_x + pill_w):
            o = (y * W + x) * 3
            buf[o:o + 3] = bytes((int(accent[0] + (pink[0] - accent[0]) * t),
                                  int(accent[1] + (pink[1] - accent[1]) * t),
                                  int(accent[2] + (pink[2] - accent[2]) * t)))
    _draw_text(buf, W, "LIVE", pill_x + 11, pill_y + 7, 1, (255, 255, 255))
    return _png_encode(W, H, bytes(buf))

_posters_cache: dict[str, bytes] = {}

def _poster_get(name: str) -> bytes:
    key = name.lower()
    png = _posters_cache.get(key)
    if png is None:
        png = _poster_png(name)
        if len(_posters_cache) > 300:
            _posters_cache.clear()
        _posters_cache[key] = png
    return png

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, code: int, body: bytes, ctype: str, cache: bool = False):
        global _request_count, _error_count
        now_min = int(time.time() // 60) * 60
        with _stats_lock:
            _request_count += 1
            if code >= 400:
                _error_count += 1
            if _hist and _hist[-1][0] == now_min:
                _hist[-1][1] += 1
            else:
                _hist.append([now_min, 1])
                while len(_hist) > 1 and _hist[0][0] < now_min - _HIST_KEEP_MIN:
                    _hist.pop(0)
                _hist_save()
            if not self.path.startswith(("/api/stats", "/api/logs", "/api/activity")):
                _request_log.append({
                    "t": time.strftime("%H:%M:%S"),
                    "method": self.command,
                    "path": self.path.split("?", 1)[0],
                    "code": code,
                    "ip": self._client_ip(),
                })
                if len(_request_log) > 300:
                    del _request_log[:len(_request_log) - 300]
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
                _sessions_save()
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
            _sessions_save()
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

        if path == "/api/check-batch":
            if not self._require_auth():
                return
            try:
                data = json.loads(body) if body else {}
                items = data.get("items") or []
                if not isinstance(items, list):
                    return self._send(400, json.dumps({"ok": False, "error": "items requis"}).encode(), "application/json")
                items = [{"src": str(x.get("src", "dlstreams")), "id": str(x.get("id", ""))} for x in items[:200]]
            except Exception:
                return self._send(400, json.dumps({"ok": False, "error": "body invalide"}).encode(), "application/json")
            if not items:
                return self._send(400, json.dumps({"ok": False, "error": "liste vide"}).encode(), "application/json")

            def _one(it: dict) -> dict:
                src = it["src"]
                cid = it["id"]
                key = f"{src}:{cid}"
                t0 = time.time()
                try:
                    if src == "vavoo":
                        real = vavoo_resolve(_unb64u(cid))
                        if not real:
                            raise ValueError("flux introuvable")
                        _proxy_get(real, {"User-Agent": _VAVOO_UA}, timeout=10)
                    else:
                        m3u8, host = resolve(cid)
                        _proxy_get(m3u8, {"Referer": host + "/", "Origin": host}, timeout=10)
                    return {"key": key, "ok": True, "ms": int((time.time() - t0) * 1000)}
                except Exception as e:
                    return {"key": key, "ok": False, "ms": int((time.time() - t0) * 1000), "error": str(e)}

            with ThreadPoolExecutor(max_workers=10) as ex:
                results = list(ex.map(_one, items))
            n_ok = sum(1 for r in results if r["ok"])
            _log_activity("Scan de flux", f"{n_ok}/{len(results)} OK")
            return self._send(200, json.dumps({"ok": True, "results": results}).encode(), "application/json")

        if path == "/api/restart":
            if not self._require_auth():
                return
            _log_activity("Redémarrage", "demandé depuis le dashboard")
            try:
                import subprocess
                import sys as _sys
                if not os.environ.get("RENDER"):
                    flags = 0
                    if os.name == "nt":
                        flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
                    subprocess.Popen([_sys.executable, os.path.abspath(__file__)],
                                     cwd=os.path.dirname(os.path.abspath(__file__)),
                                     close_fds=True, creationflags=flags)
            except Exception:
                pass
            threading.Timer(0.5, lambda: os._exit(0)).start()
            return self._send(200, json.dumps({"success": True, "message": "Redémarrage en cours..."}).encode(), "application/json")

        return self._send(404, b"not found", "text/plain")

    def do_GET(self):
        u = urllib.parse.urlsplit(self.path)
        path = u.path
        qs = urllib.parse.parse_qs(u.query)
        try:
            if path.startswith("/poster/") and path.endswith(".png"):
                pname = urllib.parse.unquote(path[len("/poster/"):-4])
                return self._send(200, _poster_get(pname), "image/png", True)

            if path == "/dashboard" or path == "/dashboard.html":
                return self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8", True)

            if path == "/configure" or path == "/configure.html":
                return self._send(200, CONFIGURE_HTML.encode("utf-8"), "text/html; charset=utf-8", True)

            if path == "/api/logout":
                tok = self._cookie("dl_session")
                with _stats_lock:
                    _sessions.pop(tok, None)
                _sessions_save()
                resp = json.dumps({"success": True}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.send_header("Set-Cookie", "dl_session=; HttpOnly; Path=/; Max-Age=0")
                self.end_headers()
                self.wfile.write(resp)
                return

            if path == "/api/stats":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(_stats()).encode(), "application/json")

            if path == "/api/channels":
                if not self._require_auth():
                    return
                lang = qs.get("lang", [None])[0]
                return self._send(200, json.dumps(channels(lang_filter=lang)).encode(), "application/json", True)

            if path == "/api/plays":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(_plays_map()).encode(), "application/json")

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

            if path == "/api/logs":
                if not self._require_auth():
                    return
                with _stats_lock:
                    logs = list(reversed(_request_log))
                return self._send(200, json.dumps(logs).encode(), "application/json")

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

            if path == "/api/check":
                if not self._require_auth():
                    return
                src = qs.get("src", ["dlstreams"])[0]
                cid = qs.get("id", [None])[0]
                if not cid:
                    return self._send(400, json.dumps({"ok": False, "error": "id manquant"}).encode(), "application/json")
                t0 = time.time()
                try:
                    if src == "vavoo":
                        real = vavoo_resolve(_unb64u(cid))
                        if not real:
                            raise ValueError("flux introuvable")
                        _proxy_get(real, {"User-Agent": _VAVOO_UA}, timeout=10)
                        url = f"{self._self_base()}/vhls?v={_b64u(cid)}"
                    else:
                        m3u8, host = resolve(cid)
                        _proxy_get(m3u8, {"Referer": host + "/", "Origin": host}, timeout=10)
                        url = f"{self._self_base()}/hls/{cid}/index.m3u8"
                    ms = int((time.time() - t0) * 1000)
                    _log_activity("Test flux", f"#{cid} OK en {ms}ms")
                    return self._send(200, json.dumps({"ok": True, "ms": ms, "url": url}).encode(), "application/json")
                except Exception as e:
                    ms = int((time.time() - t0) * 1000)
                    _log_activity("Test flux", f"#{cid} échec ({type(e).__name__})")
                    return self._send(200, json.dumps({"ok": False, "ms": ms, "error": str(e)}).encode(), "application/json")

            if path == "/api/system":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(_system_info()).encode(), "application/json")

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
                _track_play("dlstreams", cid)
                hdr = {"Referer": host + "/", "Origin": host}
                henc = _b64u(json.dumps(hdr))
                text = _proxy_get(m3u8, hdr).decode("utf-8", "replace")
                return self._send(200, _rewrite_playlist(text, m3u8, henc, self._self_base()).encode(),
                                  "application/vnd.apple.mpegurl")

            if path == "/vhls":
                vurl = _unb64u(qs["v"][0])
                real = vavoo_resolve(vurl)
                if not real:
                    return self._send(502, b"vavoo: flux introuvable (hors-antenne ?)", "text/plain")
                _track_play("vavoo", vurl)
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
            import traceback
            traceback.print_exc()
            return self._send(502, f"resolve/proxy error: {type(e).__name__}: {e}".encode(), "text/plain")

    def do_DELETE(self):
        u = urllib.parse.urlsplit(self.path)
        path = u.path

        if path == "/api/logs":
            if not self._require_auth():
                return
            with _stats_lock:
                _request_log.clear()
            _log_activity("Logs effacés")
            return self._send(200, json.dumps({"success": True}).encode(), "application/json")

        return self._send(404, b"not found", "text/plain")

    def _manifest(self, lang_filter: str | None = None) -> dict:
        _extra = [{"name": "search", "isRequired": False}, {"name": "skip", "isRequired": False}]
        name = "Chaînes live (dlstreams + Vavoo)"
        desc = ("Chaînes TV en direct (sport, info, divertissement) via dlstreams + Vavoo, "
                "lues directement dans Stremio grâce au proxy intégré. Dashboard inclus.")
        
        if lang_filter and lang_filter != "all":
            lang_names = {"fr": "Français", "en": "English", "es": "Español", "de": "Deutsch", "it": "Italiano", "ar": "Arabe", "pt": "Português"}
            lang_name = lang_names.get(lang_filter, lang_filter)
            name = f"Chaînes live {lang_name}"
            desc = f"Chaînes TV en direct en {lang_name} (dlstreams + Vavoo), lues directement dans Stremio via le proxy intégré."
        
        return {
            "id": "st.dlstreams.proxy" + (f".{lang_filter}" if lang_filter and lang_filter != "all" else ""),
"version": _VERSION,
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
        base = self._self_base()
        slug = urllib.parse.quote(c["name"], safe="")
        poster = logo or f"{base}/poster/{slug}.png"
        lang = c.get("lang", "fr")
        lang_label = {"fr": "française", "en": "anglaise", "es": "espagnole",
                      "de": "allemande", "it": "italienne", "ar": "arabe",
                      "pt": "portugaise"}.get(lang, lang)
        genres = _genre_for(c["name"])
        desc = (f"Chaîne {c['name']} diffusée en direct, chaîne {lang_label} "
                f"disponible via {source}. Lecture directe dans Stremio grâce au proxy intégré.")
        return {"id": f"{source}:{cid}", "type": "tv", "name": c["name"],
                "poster": poster, "logo": logo or poster, "posterShape": "landscape",
                "background": poster,
                "description": desc,
                "releaseInfo": "En direct",
                "genres": genres}

def main():
    _hist_load()
    _sessions_load()
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
    srv = None
    for _ in range(10):
        try:
            srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
            break
        except OSError:
            time.sleep(0.5)
    if srv is None:
        raise SystemExit(f"impossible de lier le port {PORT}")
    threading.Thread(target=_warm_channels, daemon=True).start()
    srv.serve_forever()

def _warm_channels():
    """Précharge l'annuaire en arrière-plan : le serveur écoute tout de suite
    (health check Render immédiat), le cache se remplit sans bloquer le boot."""
    try:
        n = len(channels())
        print(f"  annuaire : {n} chaines chargees (dont {len(_POPULAR_CHANNELS)} populaires)")
    except Exception as e:
        print(f"  annuaire : erreur de chargement ({e})")

DASHBOARD_HTML = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>dlstreams — Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Rajdhani:wght@500;600;700&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.15/dist/hls.min.js"></script>
<style>
  :root {
    --bg:#0a0a0b; --surface:#111113; --surface2:#18181b; --border:rgba(255,255,255,0.06);
    --accent:#e53e3e; --accent-dim:rgba(229,62,62,0.08); --green:#48bb78;
    --text:#f0f0f0; --text2:#888; --muted:#666; --input-bg:#0a0a0b; --card-hover:rgba(255,255,255,0.02);
    --warn:#f59e0b; --error:#ef4444; --info:#60a5fa;
    --font-display:'Bebas Neue',sans-serif; --font-mono:'Rajdhani',monospace; --font-body:'DM Sans',sans-serif;
  }
  body.light {
    --bg:#f1f5f9; --surface:#ffffff; --surface2:#f8fafc; --border:rgba(0,0,0,0.08);
    --accent:#dc2626; --accent-dim:rgba(220,38,38,0.08); --green:#38a169;
    --text:#0f172a; --text2:#475569; --muted:#94a3b8; --input-bg:#f8fafc; --card-hover:rgba(0,0,0,0.02);
    --info:#2563eb;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:var(--bg); color:var(--text); font-family:var(--font-body); min-height:100vh; transition:background .3s,color .3s; }

  /* LOGIN */
  #login-screen { min-height:100vh; display:flex; align-items:center; justify-content:center; position:relative; }
  #login-screen::before { content:''; position:fixed; top:-200px; left:50%; transform:translateX(-50%);
    width:600px; height:500px; background:radial-gradient(ellipse,rgba(229,62,62,0.08) 0%,transparent 70%); pointer-events:none; }
  .login-card { background:var(--surface); border:1px solid var(--border); border-radius:16px; padding:40px; width:340px;
    box-shadow:0 24px 80px rgba(0,0,0,0.3); position:relative; z-index:1; }
  .login-logo { text-align:center; margin-bottom:12px; }
  .login-logo .logo-icon { width:64px; height:64px; margin:0 auto 10px; border-radius:16px;
    background:linear-gradient(135deg,var(--accent),#ff7a59); display:grid; place-items:center;
    font-size:28px; font-weight:800; color:#fff; box-shadow:0 8px 28px rgba(229,62,62,.4); }
  .login-title { font-family:var(--font-display); font-size:30px; font-weight:400; letter-spacing:1px; text-align:center; color:var(--text); margin-bottom:2px; }
  .login-sub { font-size:11px; color:var(--muted); text-align:center; margin-bottom:24px; letter-spacing:.5px; text-transform:uppercase; }
  .field label { display:block; font-size:12px; color:var(--text2); margin-bottom:6px; font-weight:600; }
  .field input { width:100%; background:var(--input-bg); border:1px solid var(--border); border-radius:8px; padding:11px 14px;
    color:var(--text); font-family:var(--font-mono); font-size:14px; transition:border-color .2s; margin-bottom:16px; }
  .field input:focus { outline:none; border-color:var(--accent); }
  .login-error { font-size:12px; color:var(--accent); text-align:center; margin-bottom:12px; padding:8px 12px;
    background:rgba(229,62,62,0.08); border:1px solid rgba(229,62,62,0.2); border-radius:8px; display:none; }
  .btn-primary { width:100%; background:var(--accent); color:#fff; border:none; border-radius:8px; padding:12px;
    font-family:var(--font-body); font-size:14px; font-weight:700; cursor:pointer; transition:background .2s; }
  .btn-primary:hover { background:#c53030; }
  @keyframes shake { 0%,100%{transform:translateX(0)} 20%{transform:translateX(-8px)} 40%{transform:translateX(8px)} 60%{transform:translateX(-6px)} 80%{transform:translateX(6px)} }
  .shake { animation:shake .4s; }

  /* LAYOUT */
  #dashboard { display:none; }
  #dashboard.active { display:block; }
  .layout { display:flex; min-height:100vh; }
  .sidebar { width:250px; background:var(--surface); border-right:1px solid var(--border); display:flex; flex-direction:column;
    position:fixed; top:0; bottom:0; left:0; z-index:10; overflow-y:auto; }
  .sidebar-logo { padding:18px 16px; border-bottom:1px solid var(--border); display:flex; align-items:center; justify-content:space-between; gap:8px; }
  .sidebar-logo-brand { display:flex; align-items:center; gap:10px; min-width:0; }
  .sidebar-logo-icon { width:34px; height:34px; border-radius:10px; flex-shrink:0;
    background:linear-gradient(135deg,var(--accent),#ff7a59); display:grid; place-items:center;
    font-size:16px; font-weight:800; color:#fff; }
  .sidebar-logo-text .title { font-family:var(--font-display); font-size:20px; letter-spacing:.5px; font-weight:400; color:var(--text); white-space:nowrap; }
  .sidebar-logo-text .sub { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }
  .theme-btn { background:var(--surface2); border:1px solid var(--border); border-radius:8px; width:32px; height:32px;
    display:flex; align-items:center; justify-content:center; cursor:pointer; font-size:15px; transition:all .2s; flex-shrink:0; }
  .theme-btn:hover { border-color:var(--accent); }
  .sidebar-nav { flex:1; padding:14px 10px; }
  .nav-section-label { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.8px; padding:8px 10px 6px; font-weight:700; }
  .nav-item { display:flex; align-items:center; gap:9px; padding:9px 12px; border-radius:8px; font-size:13px; font-weight:600;
    color:var(--text2); cursor:pointer; margin-bottom:2px; border:none; background:none; width:100%; text-align:left;
    font-family:var(--font-body); transition:all .15s; text-decoration:none; }
  .nav-item:hover { background:var(--surface2); color:var(--text); }
  .nav-item.active { background:var(--accent-dim); color:var(--accent); }
  .nav-badge { margin-left:auto; background:var(--accent); color:#fff; font-size:10px; font-weight:800; border-radius:20px;
    padding:1px 7px; min-width:16px; text-align:center; }
  .sidebar-bottom { padding:12px 10px; border-top:1px solid var(--border); }
  .btn-logout { width:100%; background:none; border:1px solid var(--border); border-radius:8px; padding:8px; font-size:12px;
    color:var(--muted); cursor:pointer; font-family:var(--font-body); transition:all .2s; }
  .btn-logout:hover { color:var(--accent); border-color:var(--accent); }

  .main { margin-left:250px; padding:28px 32px 60px; }
  .page { display:none; }
  .page.active { display:block; }
  .page-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:24px; flex-wrap:wrap; gap:12px; }
  .page-title { font-family:var(--font-display); font-size:34px; font-weight:400; letter-spacing:.5px; color:var(--text); margin-bottom:2px; }
  .page-sub { font-size:13px; color:var(--text2); }
  .header-actions { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }

  .btn-add, .btn-outline-sm { display:flex; align-items:center; gap:6px; border-radius:7px; padding:8px 14px; font-size:12px;
    font-weight:700; font-family:var(--font-body); cursor:pointer; transition:all .2s; text-decoration:none; }
  .btn-add { background:var(--accent-dim); border:1px solid rgba(229,62,62,0.2); color:var(--accent); }
  .btn-add:hover { background:var(--accent); color:#fff; }
  .btn-outline-sm { background:none; border:1px solid var(--border); color:var(--text2); }
  .btn-outline-sm:hover { color:var(--text); border-color:var(--text2); }

  .stats-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:24px; }
  .stat-card { position:relative; overflow:hidden; border-radius:16px; padding:18px 20px;
    background:var(--surface); border:1px solid var(--border);
    display:flex; flex-direction:column; gap:4px;
    transition:border-color .2s, transform .2s; }
  .stat-card::after { content:''; position:absolute; top:-45%; right:-18%; width:150px; height:150px; border-radius:50%;
    background:radial-gradient(circle, var(--card-glow,rgba(229,62,62,.16)), transparent 70%); pointer-events:none; }
  .stat-card:hover { transform:translateY(-2px); border-color:rgba(229,62,62,.25); }
  .stat-card.c-red { --card-glow:rgba(229,62,62,.26); --card-icon-bg:rgba(229,62,62,.14); }
  .stat-card.c-blue { --card-glow:rgba(96,165,250,.26); --card-icon-bg:rgba(96,165,250,.14); }
  .stat-card.c-purple { --card-glow:rgba(167,139,250,.26); --card-icon-bg:rgba(167,139,250,.14); }
  .stat-card.c-green { --card-glow:rgba(72,187,120,.26); --card-icon-bg:rgba(72,187,120,.14); }
  .stat-card.c-orange { --card-glow:rgba(245,158,11,.26); --card-icon-bg:rgba(245,158,11,.14); }
  .stat-card.c-blue2 { --card-glow:rgba(56,189,248,.26); --card-icon-bg:rgba(56,189,248,.14); }
  .stat-card.c-red2 { --card-glow:rgba(230,57,70,.26); --card-icon-bg:rgba(230,57,70,.14); }
  .stat-label { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:1.1px; font-weight:700; }
  .stat-value { font-size:34px; font-weight:700; color:var(--text); line-height:1.05;
    font-family:var(--font-mono); letter-spacing:.5px; padding-right:44px; }
  .stat-icon { position:absolute; top:16px; right:16px; width:34px; height:34px; border-radius:10px;
    display:flex; align-items:center; justify-content:center; font-size:16px;
    background:var(--card-icon-bg,rgba(229,62,62,.12)); }
  .stat-hint { font-size:12px; color:var(--text2); margin-top:2px; display:flex; align-items:center; gap:6px; flex-wrap:wrap; }
  .cache-badge { display:inline-flex; align-items:center; gap:6px;
    font-size:11px; padding:3px 10px; border-radius:20px;
    border:1px solid var(--border); color:var(--text2); }
  .cache-badge .dot { width:7px; height:7px; border-radius:50%; background:currentColor; }
  .cache-badge.ok { color:var(--green); border-color:rgba(72,187,120,.4); background:rgba(72,187,120,.08); }
  .cache-badge.stale { color:var(--warn); border-color:rgba(245,158,11,.4); background:rgba(245,158,11,.08); }
  .cache-badge.old { color:var(--error); border-color:rgba(239,68,68,.4); background:rgba(239,68,68,.08); }

  .ov-grid-2 { display:grid; grid-template-columns:1.6fr 1fr; gap:20px; margin-bottom:20px; }
  .ov-chart-wrap { width:100%; }
  .ov-chart { width:100%; height:auto; display:block; }
  .ov-chart-grid { stroke:var(--border); stroke-width:1; stroke-dasharray:3 5; }
  .ov-chart-xlabel { font-size:9px; fill:var(--muted); font-family:var(--font-body); }
  .ov-split { display:flex; align-items:center; gap:24px; padding:8px 4px; }
  .ov-donut { width:132px; height:132px; flex-shrink:0; }
  .ov-donut-total { font-size:20px; font-weight:700; fill:var(--text); font-family:var(--font-mono); }
  .ov-donut-sub { font-size:8px; fill:var(--muted); text-transform:uppercase; letter-spacing:1px; font-weight:700; font-family:var(--font-body); }
  .ov-split-legend { display:flex; flex-direction:column; gap:10px; }
  .ov-legend-item { display:flex; align-items:center; gap:9px; font-size:13px; color:var(--text2); font-weight:600; }
  .ov-legend-item b { margin-left:auto; font-family:var(--font-mono); font-size:16px; color:var(--text); }
  .ov-legend-dot { width:10px; height:10px; border-radius:3px; flex-shrink:0; }
  .chart-bar-row { display:grid; grid-template-columns:120px 1fr 44px; align-items:center; gap:12px; }
  .chart-bar-row .chart-label { font-size:13px; color:var(--text2); text-align:right; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .chart-bar-row .chart-bar { height:18px; background:var(--surface2); border-radius:6px; overflow:hidden; }
  .chart-bar-row .chart-fill { height:100%; border-radius:6px; width:0; background:var(--accent); opacity:.85; transition:width .8s cubic-bezier(.22,1,.36,1); }
  .chart-bar-row .chart-count { font-size:12px; color:var(--text2); text-align:right; font-family:var(--font-mono); }

  .card { background:var(--surface); border:1px solid var(--border); border-radius:14px; overflow:hidden; margin-bottom:20px; }
  .card-head { display:flex; align-items:center; justify-content:space-between; padding:16px 20px; border-bottom:1px solid var(--border);
    flex-wrap:wrap; gap:8px; background:linear-gradient(180deg, rgba(230,57,70,0.05), rgba(230,57,70,0)); }
  .card-title { font-size:15px; font-weight:800; color:var(--text); letter-spacing:.01em; }
  .card-body { padding:20px; }
  .card-desc { font-size:12px; color:var(--text2); margin-top:2px; }

  .search-bar { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .search-bar input[type="search"], .search-bar input[type="text"] { flex:1; min-width:180px; background:var(--input-bg);
    border:1px solid var(--border); border-radius:8px; padding:9px 13px; color:var(--text); font-size:13px; font-family:var(--font-body); }
  .search-bar input:focus, .search-bar select:focus { outline:none; border-color:var(--accent); }
  .search-bar select:disabled { opacity:.45; cursor:not-allowed; }
  .search-bar select { background:var(--input-bg); border:1px solid var(--border); border-radius:8px; padding:9px 12px;
    color:var(--text); cursor:pointer; font-family:var(--font-body); font-size:13px; }
  .tabs { display:flex; gap:6px; }
  .tab { padding:8px 14px; border-radius:8px; border:1px solid var(--border);
    background:transparent; color:var(--text2); cursor:pointer; transition:all .15s; font-size:12px; font-weight:700; font-family:var(--font-body); }
  .tab:hover { color:var(--text); background:var(--surface2); }
  .tab.active { background:var(--accent); color:#fff; border-color:var(--accent); }

  .channel-list { display:grid; grid-template-columns:repeat(auto-fill,minmax(250px,1fr)); gap:10px; }
  .list-count { font-size:12px; color:var(--muted); margin-bottom:12px; font-weight:600; }
  .channel-item { display:flex; align-items:center; gap:10px; padding:11px 12px;
    border:1px solid var(--border); border-radius:10px;
    background:var(--surface2); cursor:pointer; transition:all .15s;
    text-decoration:none; color:var(--text); }
  .channel-item:hover { border-color:var(--accent); transform:translateY(-1px); background:var(--card-hover); }
  .channel-item .logo { width:28px; height:28px; border-radius:6px; object-fit:cover; background:#000; flex-shrink:0; }
  .channel-item .name { flex:1; font-size:13px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .channel-item .id { color:var(--muted); font-size:11px; font-family:var(--font-mono); }
  .fav-star { font-size:16px; color:var(--text2); cursor:pointer; transition:transform .2s,color .2s; user-select:none; padding:2px 6px; }
  .fav-star:hover { transform:scale(1.25); color:var(--warn); }
  .fav-star.active { color:var(--warn); text-shadow:0 0 12px rgba(245,158,11,.6); }
  .check-btn { display:inline-flex; align-items:center; gap:5px;
    padding:4px 10px; border:1px solid var(--border);
    border-radius:7px; background:transparent; color:var(--text2);
    font-size:11px; font-weight:700; cursor:pointer; transition:all .2s; white-space:nowrap; font-family:var(--font-body); }
  .check-btn:hover { border-color:var(--accent); color:var(--text); }
  .check-btn.ok { border-color:var(--green); color:var(--green); background:rgba(72,187,120,.08); }
  .check-btn.ko { border-color:var(--error); color:var(--error); background:rgba(239,68,68,.08); }
  .check-btn.busy { pointer-events:none; opacity:.6; }
  .mini-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(250px,1fr)); gap:10px; }
  .fav-empty { grid-column:1/-1; text-align:center; padding:26px; color:var(--muted); font-size:13px; }
  .fav-empty a { color:var(--accent); }

  .manual-channels-list { display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:12px; margin-top:16px; }
  .manual-channel-item { display:flex; align-items:center; justify-content:space-between;
    padding:12px; background:var(--surface2); border:1px solid var(--border);
    border-radius:10px; transition:all .15s; }
  .manual-channel-item:hover { border-color:var(--accent); }
  .manual-channel-info { flex:1; }
  .manual-channel-name { font-size:14px; font-weight:600; margin-bottom:4px; }
  .manual-channel-meta { font-size:11px; color:var(--muted); }
  .remove-btn { padding:6px 12px; background:rgba(239,68,68,0.1);
    border:1px solid var(--error); border-radius:6px;
    color:var(--error); cursor:pointer; font-size:12px; transition:all .2s; font-family:var(--font-body); }
  .remove-btn:hover { background:rgba(239,68,68,0.2); }
  .add-source-box { background:var(--input-bg); border:2px dashed var(--border);
    border-radius:12px; padding:20px; margin-bottom:14px; transition:all .3s; }
  .add-source-box:hover { border-color:var(--accent); }
  .add-source-input { width:100%; background:var(--surface); border:1px solid var(--border);
    border-radius:10px; padding:12px 16px; color:var(--text);
    font-size:13px; font-family:var(--font-mono); margin-bottom:12px; transition:all .3s; }
  .add-source-input:focus { outline:none; border-color:var(--accent); }
  .add-source-btn { padding:12px 24px; background:var(--accent); border:none; border-radius:10px; color:#fff;
    font-weight:700; font-size:13px; cursor:pointer; transition:all .2s; font-family:var(--font-body); }
  .add-source-btn:hover { background:#c53030; }
  .add-source-btn:disabled { opacity:.6; cursor:not-allowed; }
  .add-source-result { margin-top:12px; font-size:13px; }
  .alert { padding:10px 14px; border-radius:8px; margin-top:8px; font-size:12px; }
  .alert-success { background:rgba(72,187,120,0.12); border:1px solid var(--green); color:var(--green); }
  .alert-error { background:rgba(239,68,68,0.12); border:1px solid var(--error); color:var(--error); }

  .player-modal { position:fixed; top:0; left:0; right:0; bottom:0;
    background:rgba(0,0,0,0.95); z-index:1000;
    display:none; align-items:center; justify-content:center; padding:20px; }
  .player-modal.active { display:flex; }
  .player-container { width:100%; max-width:1200px; background:var(--surface);
    border:1px solid var(--border); border-radius:16px; overflow:hidden;
    box-shadow:0 20px 60px rgba(0,0,0,0.5); }
  .player-header { display:flex; justify-content:space-between; align-items:center;
    padding:16px 20px; border-bottom:1px solid var(--border); }
  .player-header h3 { margin:0; font-size:15px; font-weight:700; }
  .player-close { background:none; border:none; color:var(--text2); font-size:24px; cursor:pointer; width:32px; height:32px;
    display:grid; place-items:center; border-radius:6px; transition:all .2s; }
  .player-close:hover { background:var(--surface2); color:var(--text); }
  .player-body { padding:20px; }
  .player-frame { width:100%; aspect-ratio:16/9; background:#000; border-radius:8px; border:none; }

  /* LOGS — terminal live-tail */
  .logs-toolbar { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
  .logs-group { display:flex; align-items:center; gap:6px; }
  .logs-group label { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.6px; font-weight:700; }
  .logs-select, .logs-search input { background:var(--surface); border:1px solid var(--border); border-radius:7px; padding:7px 10px;
    font-size:12px; font-weight:600; color:var(--text); font-family:var(--font-body); cursor:pointer; }
  .logs-search input { cursor:text; min-width:160px; font-weight:500; }
  .logs-search input:focus, .logs-select:focus { outline:none; border-color:var(--accent); }
  .logs-pausebtn { display:flex; align-items:center; gap:6px; background:var(--surface2); border:1px solid var(--border); border-radius:7px;
    padding:7px 12px; font-size:12px; font-weight:700; color:var(--text2); cursor:pointer; font-family:var(--font-body); transition:all .15s; }
  .logs-pausebtn:hover { color:var(--text); border-color:var(--text2); }
  .logs-pausebtn.paused { background:var(--accent-dim); border-color:rgba(229,62,62,0.3); color:var(--accent); }
  .logs-status { display:flex; align-items:center; gap:7px; font-size:11px; color:var(--muted); font-weight:700; text-transform:uppercase; letter-spacing:.5px; }
  .logs-dot { width:8px; height:8px; border-radius:50%; background:var(--green); animation:logsPulse 1.6s infinite; flex-shrink:0; }
  .logs-dot.paused { background:var(--muted); animation:none; }
  @keyframes logsPulse { 0%,100%{opacity:1} 50%{opacity:.35} }
  .logs-term { height:520px; overflow-y:auto; background:var(--bg); font-family:var(--font-mono);
    font-size:13px; line-height:1.7; border-radius:12px; }
  .logs-term::-webkit-scrollbar { width:8px; }
  .logs-term::-webkit-scrollbar-thumb { background:var(--border); border-radius:8px; }
  .log-row { display:flex; flex-wrap:wrap; align-items:baseline; gap:10px; padding:6px 16px; border-bottom:1px solid var(--border); border-left:3px solid transparent; }
  .log-row:hover { background:var(--card-hover); }
  .log-row.row-warn { border-left-color:#f59e0b; background:rgba(245,158,11,0.04); }
  .log-row.row-err { border-left-color:var(--accent); background:rgba(230,57,70,0.05); }
  .log-time { color:var(--muted); flex-shrink:0; font-size:12px; }
  .log-method { font-weight:800; flex-shrink:0; min-width:42px; text-align:center; font-size:11px; padding:1px 6px; border-radius:4px; }
  .log-method.GET { color:var(--info); background:rgba(96,165,250,.12); }
  .log-method.POST { color:var(--warn); background:rgba(245,158,11,.12); }
  .log-code { min-width:30px; text-align:right; font-weight:700; flex-shrink:0; }
  .log-code.ok { color:var(--green); }
  .log-code.warn { color:var(--warn); }
  .log-code.err { color:var(--error); }
  .log-ip { color:var(--muted); flex-shrink:0; }
  .log-path { flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-weight:600; }
  .logs-empty { display:flex; flex-direction:column; align-items:center; justify-content:center; height:100%;
    color:var(--muted); font-family:var(--font-body); gap:8px; }
  .logs-empty .icon { font-size:32px; opacity:.4; }

  .update-time { font-size:12px; color:var(--muted); display:flex; align-items:center; gap:8px; white-space:nowrap; font-weight:600; }
  .update-time .spinner { width:13px; height:13px; border:2px solid var(--border);
    border-top-color:var(--accent); border-radius:50%; animation:spin .8s linear infinite; opacity:0; }
  .update-time.loading .spinner { opacity:1; }
  @keyframes spin { to { transform:rotate(360deg); } }
  .status-indicator { display:flex; align-items:center; gap:8px;
    padding:6px 12px; border-radius:20px;
    background:rgba(72,187,120,0.08); border:1px solid rgba(72,187,120,.4);
    color:var(--green); font-size:12px; font-weight:600; }
  .status-dot { width:8px; height:8px; border-radius:50%; background:var(--green); animation:pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }

  .toast { position:fixed; bottom:24px; right:24px; background:var(--surface); border:1px solid var(--border); border-radius:10px;
    padding:12px 18px; font-size:13px; font-weight:600; color:var(--text); box-shadow:0 8px 32px rgba(0,0,0,0.3);
    opacity:0; transform:translateY(10px); transition:all .3s; z-index:200; }
  .toast.show { opacity:1; transform:translateY(0); }
  .toast.success { border-color:var(--green); }
  .toast.error { border-color:var(--error); }
  .toast.warn { border-color:var(--warn); }

  footer { margin-top:40px; color:var(--muted); font-size:12px; text-align:center; }

  /* Raccourcis & Liens sidebar */
  .nav-shortcut { display:flex; align-items:center; gap:8px; padding:7px 12px; border-radius:8px;
    font-size:12px; font-weight:600; color:var(--text2); cursor:pointer; margin-bottom:2px;
    border:none; background:none; width:100%; text-align:left; font-family:var(--font-body); transition:all .15s; }
  a.nav-shortcut { text-decoration:none; }
  .nav-shortcut:hover { background:var(--surface2); color:var(--text); }
  .nav-shortcut.active { background:var(--surface2); color:var(--text); box-shadow: inset 2px 0 0 var(--accent); }
  .nav-shortcut .sc-ico { width:20px; text-align:center; flex-shrink:0; font-size:13px; }
  .nav-shortcut .sc-txt { flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .nav-shortcut .sc-count { font-size:10px; font-weight:800; color:var(--muted); background:var(--surface2);
    border:1px solid var(--border); border-radius:20px; padding:1px 7px; font-family:var(--font-mono); flex-shrink:0; }
  .nav-shortcut.active .sc-count { color:var(--accent); border-color:rgba(229,62,62,.35); background:rgba(229,62,62,.08); }

  /* Sélecteur de plage du graphique trafic */
  .chart-range { display:flex; gap:4px; background:var(--surface2); border:1px solid var(--border); border-radius:8px; padding:3px; }
  .range-btn { border:none; background:transparent; color:var(--muted); font-size:11px; font-weight:800;
    padding:5px 11px; border-radius:6px; cursor:pointer; font-family:var(--font-body); transition:all .15s; }
  .range-btn:hover { color:var(--text); }
  .range-btn.active { background:var(--accent); color:#fff; }

  /* Top chaînes */
  .top-row { display:grid; grid-template-columns:minmax(0,1fr) 1.2fr 44px; align-items:center; gap:12px;
    padding:8px 0; border-bottom:1px solid var(--border); cursor:pointer; }
  .top-row:last-child { border-bottom:none; }
  .top-row:hover { background:var(--card-hover); }
  .top-name { font-size:13px; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .top-bar { height:8px; background:var(--surface2); border-radius:6px; overflow:hidden; }
  .top-bar-fill { height:100%; background:var(--accent); border-radius:6px; opacity:.85; transition:width .8s cubic-bezier(.22,1,.36,1); }
  .top-plays { font-size:12px; color:var(--text2); text-align:right; font-family:var(--font-mono); font-weight:700; }
  .top-time { font-size:12px; color:var(--text2); font-weight:600; }

  /* Favoris : actions + badge scan */
  .fav-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .fav-chk { font-size:12px; font-weight:800; flex-shrink:0; }
  .fav-chk.ok { color:var(--green); }
  .fav-chk.ko { color:var(--error); }
  .scan-status { padding:10px 20px; font-size:13px; color:var(--text2); border-bottom:1px solid var(--border); display:flex; align-items:center; gap:10px; }
  .scan-status .scan-spin { width:13px; height:13px; border:2px solid var(--border); border-top-color:var(--accent);
    border-radius:50%; animation:spin .8s linear infinite; }
  .scan-status b { color:var(--text); }
  .scan-ok { color:var(--green); font-weight:800; }
  .scan-ko { color:var(--error); font-weight:800; }

  /* Page Système */
  .sys-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; }
  .sys-row { display:flex; justify-content:space-between; gap:12px; padding:10px 14px;
    background:var(--surface2); border:1px solid var(--border); border-radius:10px; }
  .sys-key { font-size:12px; color:var(--muted); font-weight:600; text-transform:uppercase; letter-spacing:.5px; }
  .sys-val { font-size:13px; color:var(--text); font-weight:700; font-family:var(--font-mono); text-align:right; word-break:break-all; }
  .btn-danger { display:inline-flex; align-items:center; gap:6px; background:rgba(239,68,68,0.12);
    border:1px solid var(--error); color:var(--error); border-radius:8px; padding:10px 18px;
    font-size:13px; font-weight:700; cursor:pointer; font-family:var(--font-body); transition:all .2s; }
  .btn-danger:hover { background:var(--error); color:#fff; }
  .btn-danger:disabled { opacity:.6; cursor:not-allowed; }

  @media (max-width:900px) {
    .sidebar { display:none; }
    .main { margin-left:0; padding:20px 16px 60px; }
    .stats-grid { grid-template-columns:repeat(2,1fr); }
    .ov-grid-2 { grid-template-columns:1fr; }
  }
</style>
</head>
<body>

  <div id="login-screen">
    <div class="login-card">
      <div class="login-logo">
        <div class="logo-icon">▶</div>
        <div class="login-title">dlstreams</div>
      </div>
      <div class="login-sub">Accès dashboard</div>
      <div class="login-error" id="login-error">Mot de passe incorrect</div>
      <div class="field">
        <label>Mot de passe</label>
        <input type="password" id="login-password" placeholder="••••••••" onkeydown="if(event.key==='Enter')handleLogin(event)">
      </div>
      <button class="btn-primary" onclick="handleLogin(event)">Se connecter</button>
    </div>
  </div>

  <div id="dashboard">
    <div class="layout">
      <div class="sidebar">
        <div class="sidebar-logo">
          <div class="sidebar-logo-brand">
            <div class="sidebar-logo-icon">▶</div>
            <div class="sidebar-logo-text">
              <div class="title">dlstreams</div>
              <div class="sub">Dashboard</div>
            </div>
          </div>
          <button class="theme-btn" id="theme-btn" onclick="toggleTheme()" title="Basculer le thème">🌙</button>
        </div>
        <div class="sidebar-nav">
          <div class="nav-section-label">Menu</div>
          <button class="nav-item active" data-page="dashboard" onclick="navigateTo('dashboard')">📊 Vue d'ensemble</button>
          <button class="nav-item" data-page="catalog" onclick="navigateTo('catalog')">📺 Catalogue</button>
          <button class="nav-item" data-page="sources" onclick="navigateTo('sources')">📡 Sources</button>
          <button class="nav-item" data-page="logs" onclick="navigateTo('logs')">📋 Logs<span class="nav-badge" id="logs-badge" style="display:none">0</span></button>
          <button class="nav-item" data-page="system" onclick="navigateTo('system')">🖥️ Système</button>
          <a class="nav-item" href="/configure" title="Choisir la langue par défaut de l'addon">🎨 Configuration</a>
          <div class="nav-section-label">Raccourcis</div>
          <button class="nav-shortcut" onclick="sidebarAction('scan')" title="Teste la disponibilité de toutes vos chaînes favorites"><span class="sc-ico">🩺</span><span class="sc-txt">Tester mes favoris</span></button>
          <button class="nav-shortcut" onclick="sidebarAction('m3u-favs')" title="Télécharge une playlist de vos favoris"><span class="sc-ico">⬇️</span><span class="sc-txt">Export favoris M3U</span></button>
          <button class="nav-shortcut" onclick="sidebarAction('m3u-catalog')" title="Télécharge une playlist du catalogue filtré"><span class="sc-ico">⬇️</span><span class="sc-txt">Export catalogue M3U</span></button>
          <button class="nav-shortcut" onclick="sidebarAction('logs-clear')" title="Efface le journal des requêtes"><span class="sc-ico">🧹</span><span class="sc-txt">Vider les logs</span></button>
          <button class="nav-shortcut" onclick="sidebarAction('restart')" title="Redémarre le processus serveur"><span class="sc-ico">🔁</span><span class="sc-txt">Redémarrer</span></button>
          <div class="nav-section-label">Langues</div>
          <div id="lang-list"><button class="nav-shortcut" disabled style="opacity:.5;cursor:default"><span class="sc-txt">chargement…</span></button></div>
        </div>
        <div class="sidebar-bottom">
          <button class="btn-logout" onclick="logout()">Déconnexion</button>
        </div>
      </div>

      <div class="main">

        <!-- PAGE: OVERVIEW -->
        <div class="page active" id="page-dashboard">
          <div class="page-header">
            <div>
              <div class="page-title">Vue d'ensemble</div>
              <div class="page-sub">Statistiques de votre proxy de chaînes</div>
            </div>
            <div class="header-actions">
              <span class="update-time" id="update-time"><div class="spinner"></div><span id="update-label">Mise à jour…</span></span>
              <div class="status-indicator"><div class="status-dot"></div><span>En ligne</span></div>
              <button class="btn-outline-sm" onclick="refreshAll()">↻ Actualiser</button>
            </div>
          </div>

          <div class="stats-grid">
            <div class="stat-card c-red">
              <div class="stat-icon">📡</div>
              <div class="stat-label">Chaînes dlstreams</div>
              <div class="stat-value" id="c-dl">—</div>
              <div class="stat-hint" id="c-dl-h"><span class="cache-badge"><span class="dot"></span>chargement…</span></div>
            </div>
            <div class="stat-card c-blue">
              <div class="stat-icon">📺</div>
              <div class="stat-label">Chaînes Vavoo</div>
              <div class="stat-value" id="c-vv">—</div>
              <div class="stat-hint" id="c-vv-h"><span class="cache-badge"><span class="dot"></span>chargement…</span></div>
            </div>
            <div class="stat-card c-green">
              <div class="stat-icon">➕</div>
              <div class="stat-label">Sources manuelles</div>
              <div class="stat-value" id="c-manual">0</div>
              <div class="stat-hint">ajoutées par vous</div>
            </div>
            <div class="stat-card c-purple">
              <div class="stat-icon">⏱️</div>
              <div class="stat-label">Uptime</div>
              <div class="stat-value" id="c-up">—</div>
              <div class="stat-hint">depuis démarrage</div>
            </div>
            <div class="stat-card c-blue2">
              <div class="stat-icon">🔁</div>
              <div class="stat-label">Requêtes</div>
              <div class="stat-value" id="c-req">—</div>
              <div class="stat-hint">total depuis démarrage</div>
            </div>
            <div class="stat-card c-red2">
              <div class="stat-icon">⚠️</div>
              <div class="stat-label">Erreurs</div>
              <div class="stat-value" id="c-err">—</div>
              <div class="stat-hint" id="c-err-h">requêtes en échec</div>
            </div>
          </div>

          <div class="ov-grid-2">
            <div class="card">
              <div class="card-head">
                <div class="card-title">Trafic</div>
                <div class="chart-range">
                  <button class="range-btn active" data-range="60" onclick="setChartRange(60)">1h</button>
                  <button class="range-btn" data-range="1440" onclick="setChartRange(1440)">24h</button>
                  <button class="range-btn" data-range="10080" onclick="setChartRange(10080)">7j</button>
                </div>
                <span class="card-desc" id="chart-total"></span>
              </div>
              <div class="card-body"><div class="ov-chart-wrap" id="traffic-chart"></div></div>
            </div>
            <div class="card">
              <div class="card-head"><div class="card-title">Répartition par langue</div></div>
              <div class="card-body" id="lang-split"></div>
            </div>
          </div>

          <div class="card">
            <div class="card-head">
              <div class="card-title">🔥 Lectures</div>
              <div class="chart-range">
                <button class="range-btn active" data-chtab="top" onclick="setChTab('top')">Top</button>
                <button class="range-btn" data-chtab="recent" onclick="setChTab('recent')">Récents</button>
              </div>
              <span class="card-desc" id="top-total"></span>
            </div>
            <div class="card-body" id="top-channels">
              <div class="fav-empty">aucune lecture pour le moment — ouvre une chaîne !</div>
            </div>
            <div class="card-body" id="top-more-wrap" style="display:none;padding-top:0">
              <button class="btn-outline-sm" id="top-more" onclick="toggleTopLimit()" style="width:100%">Voir plus</button>
            </div>
          </div>

          <div class="card">
            <div class="card-head">
              <div class="card-title">⭐ Favoris</div>
              <div class="fav-actions">
                <button class="btn-outline-sm" id="scan-favs-btn" onclick="scanFavs()">🩺 Tester mes favoris</button>
                <button class="btn-outline-sm" onclick="exportFavsM3U()">⬇️ M3U</button>
                <div class="search-bar" style="min-width:220px"><input type="search" id="fav-q" placeholder="Filtrer mes favoris…"></div>
              </div>
            </div>
            <div class="scan-status" id="scan-status" style="display:none"></div>
            <div class="card-body">
              <div class="mini-grid" id="fav-list">
                <div class="fav-empty">Aucun favori — va dans le <a href="#" onclick="navigateTo('catalog');return false">Catalogue</a> et clique sur ★ pour épingler une chaîne</div>
              </div>
            </div>
          </div>
        </div>

        <!-- PAGE: LOGS -->
        <div class="page" id="page-logs">
          <div class="page-header">
            <div>
              <div class="page-title">Logs</div>
              <div class="page-sub">Journal en direct des requêtes passées sur votre proxy (300 dernières entrées)</div>
            </div>
          </div>

          <div class="card">
            <div class="card-head logs-toolbar">
              <div class="logs-group">
                <label>Méthode</label>
                <select class="logs-select" id="logs-method" onchange="renderLogs()">
                  <option value="">Toutes</option>
                  <option value="GET">GET</option>
                  <option value="POST">POST</option>
                  <option value="DELETE">DELETE</option>
                </select>
              </div>
              <div class="logs-group logs-search">
                <input type="text" id="logs-search" placeholder="Rechercher une route ou une IP..." oninput="renderLogs()">
              </div>
              <div class="logs-group">
                <label>Rafraîchissement</label>
                <select class="logs-select" id="logs-interval" onchange="restartLogPolling()">
                  <option value="2000">2s</option>
                  <option value="5000" selected>5s</option>
                  <option value="10000">10s</option>
                  <option value="0">Off</option>
                </select>
              </div>
              <button class="logs-pausebtn" id="logs-pausebtn" onclick="toggleLogPause()">⏸️ Pause</button>
              <button class="btn-outline-sm" onclick="exportLogs()">⬇️ Exporter</button>
              <button class="btn-outline-sm" onclick="clearLogs()">🗑️ Vider</button>
              <div class="logs-status" style="margin-left:auto">
                <div class="logs-dot" id="logs-dot"></div>
                <span id="logs-statustext">Live</span>
              </div>
            </div>
            <div class="logs-term" id="logs-term">
              <div class="logs-empty"><div class="icon">📋</div><p>En attente de logs...</p></div>
            </div>
          </div>
        </div>

        <!-- PAGE: SYSTEM -->
        <div class="page" id="page-system">
          <div class="page-header">
            <div>
              <div class="page-title">Système</div>
              <div class="page-sub">Infos serveur, cache et redémarrage</div>
            </div>
            <div class="header-actions">
              <button class="btn-outline-sm" onclick="loadSystem()">↻ Actualiser</button>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">🖥️ Serveur</div></div>
            <div class="card-body">
              <div class="sys-grid" id="sys-grid"><div class="fav-empty">chargement…</div></div>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">🗂️ Cache</div></div>
            <div class="card-body">
              <div class="sys-grid" id="sys-cache"><div class="fav-empty">chargement…</div></div>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">🔄 Redémarrage</div></div>
            <div class="card-body">
              <p style="color:var(--text2);font-size:13px;margin-bottom:14px">Redémarre le processus serveur. Sur Render, la plateforme le relance automatiquement.</p>
              <button class="btn-danger" id="restart-btn" onclick="restartServer()">🔁 Redémarrer le serveur</button>
            </div>
          </div>
        </div>

        <!-- PAGE: SOURCES -->
        <div class="page" id="page-sources">
          <div class="page-header">
            <div>
              <div class="page-title">Sources</div>
              <div class="page-sub">Gérer vos sources personnalisées</div>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">📡 Ajouter une source</div></div>
            <div class="card-body">
              <div class="add-source-box">
                <input class="add-source-input" id="source-url" type="url" placeholder="Collez l'URL d'une page dlstreams (ex: https://dlstreams.st/watch.php?id=121)">
                <button class="add-source-btn" id="add-source-btn">🔍 Scraper & Ajouter</button>
                <div class="add-source-result" id="add-source-result"></div>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">📋 Sources ajoutées manuellement</div></div>
            <div class="card-body">
              <p style="color:var(--text2);font-size:13px;margin-bottom:16px">Ces chaînes ont été ajoutées via le scraper et sont conservées en mémoire.</p>
              <div class="manual-channels-list" id="manual-channels-list">
                <div style="color:var(--muted);text-align:center;padding:30px;grid-column:1/-1">Aucune source ajoutée</div>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">🕒 Activité récente</div></div>
            <div class="card-body">
              <div id="activity-list-src" style="font-size:13px;color:var(--text2)">chargement…</div>
            </div>
          </div>
        </div>

        <!-- PAGE: CATALOG -->
        <div class="page" id="page-catalog">
          <div class="page-header">
            <div>
              <div class="page-title">Catalogue</div>
              <div class="page-sub">Explorer toutes les chaînes disponibles</div>
            </div>
          </div>

          <div class="card">
            <div class="card-head">
              <div class="search-bar" style="flex:1">
                <input type="search" id="q" placeholder="Rechercher une chaîne (ex : beIN, Canal+, RMC Sport…)">
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
                <select id="catalog-sort" onchange="setCatalogSort()">
                  <option value="name">Tri : Nom</option>
                  <option value="plays">Tri : Lectures</option>
                </select>
                <div class="tabs">
                  <button class="tab active" data-src="dlstreams">dlstreams</button>
                  <button class="tab" data-src="vavoo">Vavoo</button>
                </div>
                <button class="btn-outline-sm" onclick="exportCatalogM3U()">⬇️ M3U</button>
              </div>
            </div>
            <div class="card-body">
              <div class="list-count" id="catalog-count"></div>
              <div class="channel-list" id="list"><div style="color:var(--muted);text-align:center;padding:30px;grid-column:1/-1">chargement…</div></div>
            </div>
          </div>
        </div>

      </div>
    </div>
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

  <div class="toast" id="toast"></div>

<script>
const BASE = location.origin;
const $ = s => document.querySelector(s);
const fmtDur = s => {
    if (s==null) return "—";
    const d=Math.floor(s/86400), h=Math.floor(s%86400/3600), m=Math.floor(s%3600/60);
    return (d?d+"j ":"")+(h?h+"h ":"")+(m+"m");
};
const fmtAge = s => s==null ? "pas encore chargé" : (s<60?s+"s":Math.floor(s/60)+"min");

// Theme clair/sombre, persisté en localStorage
function applyTheme(t){
    document.body.classList.toggle("light", t==="light");
    localStorage.setItem("dl_theme", t);
    $("#theme-btn").textContent = t==="light" ? "☀️" : "🌙";
}
function toggleTheme(){
    applyTheme(localStorage.getItem("dl_theme")==="light" ? "dark" : "light");
}
applyTheme(localStorage.getItem("dl_theme") || "dark");

// Session verifiee cote serveur via cookie httponly
async function checkSession() {
    try {
        const r = await fetch("/api/stats");
        if (r.ok) {
            $('#login-screen').style.display = 'none';
            $('#dashboard').classList.add('active');
            await boot();
        }
    } catch (e) { /* pas connecte */ }
}

async function apiFetch(url, opts) {
    const r = await fetch(url, opts);
    if (r.status === 401) {
        $('#dashboard').classList.remove('active');
        $('#login-screen').style.display = 'flex';
        toast('Session expirée, reconnecte-toi', 'error');
        throw new Error('unauthenticated');
    }
    return r;
}

function toast(msg, type) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.className = 'toast show' + (type ? ' ' + type : '');
    clearTimeout(toast._t);
    toast._t = setTimeout(() => t.classList.remove('show'), 2800);
}

function handleLogin(e) {
    if (e) e.preventDefault();
    const password = $('#login-password').value;
    const err = document.getElementById('login-error');
    err.style.display = 'none';
    fetch('/api/auth', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({password})
    })
        .then(r => r.json().then(data => ({ok: r.ok, data})))
        .then(({ok, data}) => {
            if (ok && data.success) {
                err.innerHTML = '';
                $('#login-screen').style.display = 'none';
                $('#dashboard').classList.add('active');
                toast('✅ Connecté avec succès', 'success');
                boot();
            } else {
                err.textContent = data.message || 'Mot de passe incorrect';
                err.style.display = 'block';
                const card = document.querySelector('.login-card');
                card.classList.remove('shake'); void card.offsetWidth; card.classList.add('shake');
            }
        })
        .catch(() => { err.textContent = 'Erreur réseau'; err.style.display = 'block'; });
}

function logout() {
    fetch('/api/logout').finally(() => {
        $('#dashboard').classList.remove('active');
        $('#login-screen').style.display = 'flex';
        $('#login-password').value = '';
        toast('👋 Déconnecté');
    });
}

let _lastPage = null;
function navigateTo(page) {
    _lastPage = page;
    try { if (location.hash !== '#' + page) history.replaceState(null, '', '#' + page); } catch(e){}
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));

    $(`#page-${page}`).classList.add('active');
    document.querySelector(`[data-page="${page}"]`).classList.add('active');

    const subtitles = {
        dashboard: 'Statistiques de votre proxy de chaînes',
        sources: 'Gérer vos sources personnalisées',
        catalog: 'Explorer toutes les chaînes disponibles',
        logs: 'Journal en direct des requêtes passées sur votre proxy',
        system: 'Infos serveur, cache et redémarrage'
    };
    document.querySelector('.page.active .page-sub').textContent = subtitles[page] || '';

    if (page === 'dashboard') { renderFavs(); }
    if (page === 'sources') { loadManualChannels(); loadActivity("activity-list-src"); }
    if (page === 'logs') { loadLogs(); }
    if (page === 'system') { loadSystem(); }
    if (page === 'catalog') {
        if (!ALL.dlstreams.length) loadCatalog('dlstreams').then(render); else render();
    }
}
window.addEventListener('hashchange', () => {
    const p = location.hash.replace('#', '');
    if (p && p !== _lastPage && document.getElementById('page-' + p)) navigateTo(p);
});

async function refreshStats(){
    try{
        const r = await apiFetch("/api/stats");
        const d = await r.json();
        animateNumber($("#c-dl"), d.dlstreams.count);
        setCacheBadge($("#c-dl-h"), d.dlstreams.age_seconds);
        animateNumber($("#c-vv"), d.vavoo.count);
        setCacheBadge($("#c-vv-h"), d.vavoo.age_seconds);
        $("#c-up").textContent = fmtDur(d.uptime);
        animateNumber($("#c-manual"), d.manual_channels || 0);
        animateNumber($("#c-req"), d.requests||0);
        animateNumber($("#c-err"), d.errors || 0);
        const errH = $("#c-err-h");
        if(errH) errH.textContent = (d.requests && d.errors)
            ? Math.min(100, (d.errors / d.requests * 100)).toFixed(1) + '% des requêtes'
            : 'requêtes en échec';
        renderAreaChart(d.history || []);
        renderLangSplit(d.lang_counts || {});
        LAST_TOP = d.top_channels || [];
        LAST_REC = d.recent_plays || [];
        renderChannelsCard();
        LAST_LANG_COUNTS = d.lang_counts || {};
        renderLangShortcuts();
        const ut = $("#update-time");
        ut.classList.remove("loading");
        $("#update-label").textContent = "MAJ " + new Date().toLocaleTimeString('fr-FR');
    }catch(e){
        if (e.message !== 'unauthenticated') console.error("Stats error:", e);
    }
}

function animateNumber(el, target) {
    if (!el) return;
    const startVal = parseInt(el.textContent.replace(/\D/g,''), 10) || 0;
    const dur = 500;
    const t0 = performance.now();
    function frame(now) {
        const t = Math.min((now - t0) / dur, 1);
        const eased = 1 - Math.pow(1 - t, 3);
        el.textContent = Math.round(startVal + (target - startVal) * eased).toLocaleString('fr-FR');
        if (t < 1) requestAnimationFrame(frame);
    }
    requestAnimationFrame(frame);
}

function setCacheBadge(el, age){
    if(age == null){ el.innerHTML = '<span class="cache-badge old"><span class="dot"></span>pas encore chargé</span>'; return; }
    let cls = "ok", label = "il y a " + fmtAge(age);
    if(age > 3600){ cls = "old"; label = "périmé (" + fmtAge(age) + ")"; }
    else if(age > 600){ cls = "stale"; }
    el.innerHTML = `<span class="cache-badge ${cls}"><span class="dot"></span>${label}</span>`;
}

// Graphique en aires (SVG) du trafic, range 1h / 24h / 7j
let CHART_RANGE = 60;
let LAST_HIST = [];
function setChartRange(mins){
    CHART_RANGE = mins;
    document.querySelectorAll('.range-btn').forEach(b=>b.classList.toggle('active', Number(b.dataset.range)===mins));
    renderAreaChart(LAST_HIST);
}
function renderAreaChart(history){
    const el = $("#traffic-chart");
    if(!el) return;
    LAST_HIST = history || [];
    if(!LAST_HIST.length){ el.innerHTML = '<div class="fav-empty">aucune donnée</div>'; return; }
    const nowMin = Date.now() / 1000;
    const windowMin = CHART_RANGE === 10080 ? 10080 : (CHART_RANGE === 1440 ? 1440 : 60);
    const arr = LAST_HIST.filter(h => nowMin - h[0] <= windowMin + 1);
    const tot = $("#chart-total");
    if(tot) tot.textContent = arr.length ? arr.reduce((a,h)=>a+h[1], 0).toLocaleString('fr-FR') + ' requêtes' : '';
    if(!arr.length){ el.innerHTML = '<div class="fav-empty">aucune donnée sur cette période</div>'; return; }
    const counts = arr.map(h => h[1]);
    const W = 600, H = 170, PX = 12, PY = 24, PB = 28;
    let maxV = 1;
    for(let i=0;i<counts.length;i++) if(counts[i] > maxV) maxV = counts[i];
    const n = counts.length;
    const stepX = n > 1 ? (W - PX * 2) / (n - 1) : 0;
    const yOf = v => H - PB - (v / maxV) * (H - PY - PB);
    const pts = counts.map((v, i) => ({ x: PX + i * stepX, y: yOf(v), v, t: arr[i][0] }));
    const baseY = H - PB;
    const line = pts.map((p, i) => (i ? 'L' : 'M') + p.x.toFixed(1) + ' ' + p.y.toFixed(1)).join(' ');
    const area = line + ' L' + pts[pts.length-1].x.toFixed(1) + ' ' + baseY + ' L' + pts[0].x.toFixed(1) + ' ' + baseY + ' Z';
    const last = pts[pts.length-1];
    const grid = [0.25, 0.5, 0.75, 1].map(f => {
        const gy = baseY - f * (H - PY - PB);
        return '<line x1="'+PX+'" y1="'+gy.toFixed(1)+'" x2="'+(W-PX)+'" y2="'+gy.toFixed(1)+'" class="ov-chart-grid"/>';
    }).join('');
    const labelEvery = Math.max(1, Math.round(n / 6));
    const labels = pts.map((p, i) => {
        if (i !== n-1 && i % labelEvery !== 0) return '';
        const t = new Date(p.t * 1000);
        const fmt = windowMin >= 1440
            ? t.toLocaleDateString('fr-FR',{day:'2-digit',month:'2-digit'})
            : t.toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'});
        return '<text x="'+p.x.toFixed(1)+'" y="'+(H-8)+'" text-anchor="middle" class="ov-chart-xlabel"'+(i===n-1?' style="fill:var(--accent);font-weight:800"':'')+'>'+
            fmt+'</text>';
    }).join('');
    const dots = pts.filter((_,i)=>i%Math.max(1,Math.round(n/40))===0).map((p,i) => '<circle cx="'+p.x.toFixed(1)+'" cy="'+p.y.toFixed(1)+'" r="2.5" style="fill:var(--surface);stroke:var(--accent);stroke-width:2"><title>'+p.v+' req</title></circle>').join('');
    el.innerHTML = '<svg class="ov-chart" viewBox="0 0 '+W+' '+H+'">' +
        '<defs><linearGradient id="ov-fill" x1="0" y1="0" x2="0" y2="1">' +
        '<stop offset="0%" style="stop-color:var(--accent);stop-opacity:0.32"/>' +
        '<stop offset="100%" style="stop-color:var(--accent);stop-opacity:0"/>' +
        '</linearGradient></defs>' + grid +
        '<path d="'+area+'" fill="url(#ov-fill)"/>' +
        '<path d="'+line+'" fill="none" style="stroke:var(--accent)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>' +
        '<circle cx="'+last.x.toFixed(1)+'" cy="'+last.y.toFixed(1)+'" r="9" fill="var(--accent)" opacity="0.18"/>' +
        '<circle cx="'+last.x.toFixed(1)+'" cy="'+last.y.toFixed(1)+'" r="4" fill="var(--accent)"/>' +
        dots + labels + '</svg>';
}

// Card "Lectures" : Top chaînes / Récents
let CH_TAB = 'top', LAST_TOP = [], LAST_REC = [], _topLimit = 10;
function setChTab(t){
    CH_TAB = t;
    document.querySelectorAll('.card [data-chtab]').forEach(b=>b.classList.toggle('active', b.dataset.chtab===t));
    renderChannelsCard();
}
function toggleTopLimit(){
    _topLimit = _topLimit === 10 ? 30 : 10;
    renderChannelsCard();
}
function fmtAgo(t){
    const s = Math.max(1, Math.round((Date.now()/1000 - t)));
    if (s < 60) return 'à l\'instant';
    if (s < 3600) return 'il y a ' + Math.floor(s/60) + ' min';
    if (s < 86400) return 'il y a ' + Math.floor(s/3600) + ' h';
    return 'il y a ' + Math.floor(s/86400) + ' j';
}
function renderChannelsCard(){
    const el = $("#top-channels");
    if(!el) return;
    const moreWrap = $("#top-more-wrap");
    const tot = $("#top-total");
    const isTop = CH_TAB === 'top';
    const list = isTop ? LAST_TOP : LAST_REC;
    if(!list.length){
        el.innerHTML = '<div class="fav-empty">aucune lecture pour le moment — ouvre une chaîne !</div>';
        if(moreWrap) moreWrap.style.display = 'none';
        if(tot) tot.textContent = '';
        return;
    }
    const shown = isTop ? list.slice(0, _topLimit) : list.slice(0, 12);
    const max = isTop ? Math.max(...list.map(t=>t.plays)) : 1;
    el.innerHTML = shown.map(t => {
        const href = t.src==="vavoo" ? `${BASE}/vhls?v=${encodeURIComponent(b64u(t.id))}` : `${BASE}/hls/${t.id}/index.m3u8`;
        const bar = isTop ? Math.round((t.plays / max) * 100) : 0;
        return `<div class="top-row" data-play="${href}" title="${escapeHtml(t.name)}">
            <div class="top-name">${escapeHtml(t.name)}</div>
            ${isTop
                ? `<div class="top-bar"><div class="top-bar-fill" style="width:${bar}%"></div></div><div class="top-plays">${t.plays}</div>`
                : `<div class="top-bar"><span class="top-time">${fmtAgo(t.t)}</span></div><div class="top-plays" style="color:var(--accent)">▶</div>`}
        </div>`;
    }).join('');
    if(tot){
        tot.textContent = isTop
            ? (LAST_TOP.reduce((a,t)=>a+t.plays, 0) + ' lecture' + (LAST_TOP.length>1?'s':''))
            : 'dernière : ' + fmtAgo(LAST_REC[0].t);
    }
    if(moreWrap){
        moreWrap.style.display = isTop && list.length > 10 ? 'block' : 'none';
        const btn = $("#top-more");
        if(btn) btn.textContent = _topLimit === 10 ? `Voir plus (${list.length})` : 'Voir moins';
    }
}

// Donut de repartition par langue
function renderLangSplit(lang_counts){
    const el = $("#lang-split");
    if(!el) return;
    const flags = {fr:"🇫🇷",en:"🇬🇧",es:"🇪🇸",de:"🇩🇪",it:"🇮🇹",ar:"🇸🇦",pt:"🇵🇹",other:"📺"};
    const names = {fr:"Français",en:"English",es:"Español",de:"Deutsch",it:"Italiano",ar:"Arabe",pt:"Português",other:"Autres"};
    const colors = {fr:'#e53e3e',en:'#60a5fa',es:'#f59e0b',de:'#a78bfa',it:'#48bb78',ar:'#34d399',pt:'#38bdf8',other:'#94a3b8'};
    const entries = Object.entries(lang_counts).sort((a,b)=>b[1]-a[1]);
    const total = entries.reduce((s,e)=>s+e[1], 0);
    if(!total){ el.innerHTML = '<div class="fav-empty">aucune donnée</div>'; return; }
    const segments = entries.map(([lang,n]) => ({ label: (flags[lang]||"🌍")+' '+(names[lang]||lang), value: n, color: colors[lang]||'#94a3b8' }));
    const R = 40, C = 2 * Math.PI * R;
    let acc = 0;
    let arcs = '<circle cx="50" cy="50" r="'+R+'" fill="none" style="stroke:var(--surface2)" stroke-width="13"/>';
    segments.forEach(seg => {
        const len = (seg.value / total) * C;
        arcs += '<circle cx="50" cy="50" r="'+R+'" fill="none" style="stroke:'+seg.color+'" stroke-width="13" stroke-linecap="round" stroke-dasharray="'+len.toFixed(2)+' '+C.toFixed(2)+'" stroke-dashoffset="'+(-acc).toFixed(2)+'" transform="rotate(-90 50 50)"/>';
        acc += len;
    });
    const legend = segments.map(seg => {
        const pct = Math.round((seg.value / total) * 100);
        return '<div class="ov-legend-item"><span class="ov-legend-dot" style="background:'+seg.color+'"></span><span>'+escapeHtml(seg.label)+'</span><b>'+pct+'%</b></div>';
    }).join('');
    el.innerHTML = '<div class="ov-split">' +
        '<svg viewBox="0 0 100 100" class="ov-donut">' + arcs +
        '<text x="50" y="46" text-anchor="middle" class="ov-donut-total">'+total.toLocaleString('fr-FR')+'</text>' +
        '<text x="50" y="59" text-anchor="middle" class="ov-donut-sub">chaînes</text>' +
        '</svg>' +
        '<div class="ov-split-legend">' + legend + '</div></div>';
}

async function loadActivity(targetId) {
    try {
        const r = await apiFetch("/api/activity");
        const log = await r.json();
        const el = $("#" + (targetId || "activity-list"));
        if(!el) return;
        if (!log.length) { el.innerHTML = "aucune activité pour le moment"; return; }
        el.innerHTML = log.slice(0, 20).map(e =>
            `<div style="padding:6px 0;border-bottom:1px solid var(--border)">
                <span style="color:var(--text)">${escapeHtml(e.action)}</span>
                ${e.details ? ' — ' + escapeHtml(e.details) : ''}
                <span style="float:right;color:var(--muted)">${e.time}</span>
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
            list.innerHTML = '<div style="color:var(--muted);text-align:center;padding:30px;grid-column:1/-1">Aucune source ajoutée</div>';
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
            toast('✅ Chaîne supprimée', 'success');
            await loadManualChannels();
            await refreshStats();
        } else {
            toast('❌ Erreur: ' + d.message, 'error');
        }
    } catch(e) {
        if (e.message !== 'unauthenticated') toast('❌ Erreur: ' + e.message, 'error');
    }
}

async function refreshAll() {
    try {
        const r = await apiFetch("/api/refresh-cache");
        const d = await r.json();
        await refreshStats();
        toast(`✅ Cache rafraîchi : ${d.dlstreams} dlstreams / ${d.vavoo} vavoo`, 'success');
    } catch(e) {
        if (e.message !== 'unauthenticated') toast('❌ Erreur de rafraîchissement', 'error');
    }
}

let CURRENT = "dlstreams", ALL = {dlstreams:[], vavoo:[]};
let LANG_FILTER = localStorage.getItem("dl_lang") || "fr";
let SORT = localStorage.getItem("dl_sort") || "name";
let PLAYS = {};

async function loadPlays(){
    try{
        const r = await apiFetch("/api/plays");
        PLAYS = await r.json();
    }catch(e){
        if (e.message !== 'unauthenticated') console.error("Plays error:", e);
    }
}
function setCatalogSort(){
    SORT = $("#catalog-sort").value;
    localStorage.setItem("dl_sort", SORT);
    render();
}

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

    const matches = (ALL[CURRENT]||[]).filter(c => {
        if (lang && c.lang !== lang) return false;
        return words.every(w => (c.name||"").toLowerCase().includes(w));
    });
    if (SORT === "plays") {
        matches.sort((a,b) => (PLAYS[(CURRENT==="vavoo"?"vavoo:":"dlstreams:")+b.id]||0) - (PLAYS[(CURRENT==="vavoo"?"vavoo:":"dlstreams:")+a.id]||0));
    } else {
        matches.sort((a,b) => (a.name||"").localeCompare(b.name||"", 'fr', {sensitivity:'base'}));
    }
    const items = matches.slice(0, 300);

    const count = $("#catalog-count");
    if(count){
        count.textContent = matches.length
            ? (items.length < matches.length ? `${items.length} / ${matches.length} chaînes affichées` : `${matches.length} chaînes`)
            : "aucun résultat";
    }

    const list = $("#list");
    if(!items.length){
        list.innerHTML = '<div style="color:var(--muted);text-align:center;padding:30px;grid-column:1/-1">aucun résultat</div>';
        return;
    }
    list.innerHTML = items.map(c => {
        const encodedId = CURRENT==="vavoo" ? b64u(c.id) : c.id;
        const href = CURRENT==="vavoo"
            ? `${BASE}/vhls?v=${encodeURIComponent(encodedId)}`
            : `${BASE}/hls/${c.id}/index.m3u8`;
        const logo = c.logo ? `<img class="logo" src="${escapeHtml(c.logo)}" alt="" onerror="this.style.display='none'">` : "";
        const key = (CURRENT==="vavoo"?"vavoo:":"dlstreams:")+c.id;
        return `<a class="channel-item" href="${href}" target="_blank" title="${escapeHtml(c.name)}" data-play="${href}">
            ${logo}
            <div class="name">${escapeHtml(c.name)}</div>
            <div class="id">${CURRENT==="vavoo"?"vavoo":"#"+c.id}</div>
            <span class="check-btn ${checkCls(key)}" onclick="event.preventDefault();event.stopPropagation();checkStream('${escapeHtml(key)}')">${checkLabel(key)}</span>
            <span class="fav-star ${isFav(key)?"active":""}" onclick="event.preventDefault();event.stopPropagation();toggleFavKey('${escapeHtml(key)}')">★</span>
        </a>`;
    }).join("");
}

const CHECKED = (()=>{ try{ return JSON.parse(localStorage.getItem("dl_checked")||"{}"); }catch(e){ return {}; } })();
function saveChecked(){ try{ localStorage.setItem("dl_checked", JSON.stringify(CHECKED)); }catch(e){} }
function checkCls(key){ const s = CHECKED[key]; return s ? (s.state==="busy"?"busy":s.state) : ""; }
function checkLabel(key){
    const s = CHECKED[key];
    if(!s) return '▶ test';
    if(s.state==="busy") return '⏳…';
    if(s.state==="ok") return '✓ '+(s.ms||0)+'ms';
    return '✗ KO';
}
async function checkStream(key){
    if(CHECKED[key] && CHECKED[key].state==="busy") return;
    CHECKED[key] = {state:"busy"};
    render();
    const src = key.split(":")[0];
    const id = key.slice(key.indexOf(":")+1);
    try{
        const enc = src==="vavoo" ? b64u(id) : id;
        const r = await apiFetch(`/api/check?src=${src}&id=${encodeURIComponent(enc)}`);
        const d = await r.json();
        CHECKED[key] = {state: d.ok ? "ok" : "ko", ms: d.ms, url: d.url};
        saveChecked();
    }catch(e){
        if (e.message !== 'unauthenticated') CHECKED[key] = {state:"ko", ms:0};
    }
    render();
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
    if(i>=0){ favs.splice(i,1); toast("Retiré des favoris"); }
    else { favs.push({key, src, id: ch.id, name: ch.name, logo: ch.logo||""}); toast("⭐ Ajouté aux favoris", "success"); }
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
        const logo = f.logo ? `<img class="logo" src="${escapeHtml(f.logo)}" alt="" onerror="this.style.display='none'">` : "";
        const st = CHECKED[f.key];
        const badge = st ? `<span class="fav-chk ${st.state==="ok"?"ok":"ko"}">${st.state==="ok"?"✓":"✗"}</span>` : "";
        return `<a class="channel-item" href="${href}" data-play="${href}" title="${escapeHtml(f.name)}">
            ${logo}
            <div class="name">${escapeHtml(f.name)}</div>
            <div class="id">${f.src==="vavoo"?"vavoo":"#"+f.id}</div>
            ${badge}
            <span class="fav-star active" onclick="event.preventDefault();event.stopPropagation();removeFav('${escapeHtml(f.key)}')">★</span>
        </a>`;
    }).join("");
}
function removeFav(key){
    saveFavs(getFavs().filter(f=>f.key!==key));
    toast("Retiré des favoris");
    renderFavs();
    render();
}

// Scan de santé en masse des favoris
async function scanFavs(){
    const favs = getFavs();
    if(!favs.length){ toast('⚠️ Aucun favori à tester', 'warn'); return; }
    const tested = favs.slice(0,200);
    const btn = $("#scan-favs-btn");
    const status = $("#scan-status");
    btn.disabled = true;
    btn.textContent = "⏳ Scan en cours...";
    status.style.display = 'flex';
    status.innerHTML = '<div class="scan-spin"></div><span>Test de <b>' + tested.length + '</b> chaîne(s)' + (favs.length > 200 ? ' (sur ' + favs.length + ', max 200)' : '') + '…</span>';
    try{
        const r = await apiFetch('/api/check-batch', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({items: tested.map(f => ({src: f.src, id: f.src==="vavoo" ? b64u(f.id) : f.id}))})
        });
        const d = await r.json();
        const results = d.results || [];
        const ok = results.filter(x=>x.ok).length;
        results.forEach(res => { CHECKED[res.key] = {state: res.ok ? "ok" : "ko", ms: res.ms}; });
        saveChecked();
        status.innerHTML = '<span>Scan terminé : <b>' + ok + '</b> OK / <b>' + results.length + '</b> chaînes</span>' +
            '<span class="scan-ok">✓</span><span class="scan-ko">✗</span>';
        renderFavs();
        render();
        toast(`Scan terminé : ${ok}/${results.length} OK`, ok === results.length ? 'success' : 'warn');
    }catch(e){
        if (e.message !== 'unauthenticated') {
            status.innerHTML = '<span class="scan-ko">✗ Scan impossible : ' + escapeHtml(e.message) + '</span>';
            toast('❌ Scan impossible: ' + e.message, 'error');
        }
    }finally{
        btn.disabled = false;
        btn.textContent = "🩺 Tester mes favoris";
    }
}

// Export M3U
function downloadText(filename, text){
    const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = filename;
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
}
function buildM3U(chans){
    const lines = ['#EXTM3U'];
    chans.forEach(ch => {
        const href = ch.src==="vavoo" ? `${BASE}/vhls?v=${encodeURIComponent(b64u(ch.id))}` : `${BASE}/hls/${ch.id}/index.m3u8`;
        const grp = (ch.lang && ch.lang !== "other") ? ch.lang.toUpperCase() : "Autres";
        lines.push(`#EXTINF:-1 tvg-id="${ch.src}:${ch.id}" group-title="${grp}",${escapeHtml(ch.name)}`);
        lines.push(href);
    });
    return lines.join('\n');
}
function exportFavsM3U(){
    const favs = getFavs();
    if(!favs.length){ toast('⚠️ Aucun favori à exporter', 'warn'); return; }
    downloadText('dlstreams-favoris.m3u', buildM3U(favs));
    toast(`✅ ${favs.length} favori(s) exporté(s)`, 'success');
}
async function exportCatalogM3U(){
    if(!ALL[CURRENT].length) await loadCatalog(CURRENT);
    const q = ($("#q").value||"").toLowerCase().trim();
    const words = q ? q.split(/\s+/) : [];
    const lang = LANG_FILTER === "all" ? null : LANG_FILTER;
    const chans = (ALL[CURRENT]||[]).filter(c => {
        if (lang && c.lang !== lang) return false;
        return words.every(w => (c.name||"").toLowerCase().includes(w));
    });
    if(!chans.length){ toast('⚠️ Aucune chaîne à exporter', 'warn'); return; }
    downloadText('dlstreams-' + CURRENT + '.m3u', buildM3U(chans.map(c => Object.assign({}, c, {src: CURRENT}))));
    toast(`✅ ${chans.length} chaînes exportées`, 'success');
}

// Raccourcis sidebar : actions rapides
function sidebarAction(action){
    if(action === 'scan'){ navigateTo('dashboard'); setTimeout(()=>scanFavs(), 200); }
    else if(action === 'm3u-favs'){ exportFavsM3U(); }
    else if(action === 'm3u-catalog'){ navigateTo('catalog'); setTimeout(()=>exportCatalogM3U(), 200); }
    else if(action === 'logs-clear'){ navigateTo('logs'); setTimeout(()=>clearLogs(), 200); }
    else if(action === 'restart'){ restartServer(); }
}

// Langues sidebar : boutons dynamiques avec compteurs
const LANG_META = {
    all:   ['🌍', 'Toutes'],
    fr:    ['🇫🇷', 'Français'],
    en:    ['🇬🇧', 'English'],
    es:    ['🇪🇸', 'Español'],
    pt:    ['🇵🇹', 'Português'],
    it:    ['🇮🇹', 'Italiano'],
    de:    ['🇩🇪', 'Deutsch'],
    ar:    ['🇸🇦', 'Arabe'],
    other: ['📺', 'Autres']
};
let LAST_LANG_COUNTS = {};
function renderLangShortcuts(){
    const wrap = $("#lang-list");
    if(!wrap) return;
    const lc = LAST_LANG_COUNTS || {};
    const total = Object.values(lc).reduce((a,b)=>a+b,0);
    const withOther = lc.other ? lc.other : 0;
    const entries = [['all', total]].concat(
        Object.entries(lc).filter(([k])=>k!=='other').sort((a,b)=>b[1]-a[1])
    );
    if(withOther) entries.push(['other', withOther]);
    wrap.innerHTML = entries.map(([k,count]) => {
        const [emoji, label] = LANG_META[k] || ['🌐', k];
        const active = LANG_FILTER === k ? ' active' : '';
        return `<button class="nav-shortcut nav-lang${active}" data-lang="${k}" onclick="goLang('${k}')" title="Filtrer le catalogue : ${label} (${count})">
            <span class="sc-ico">${emoji}</span><span class="sc-txt">${label}</span><span class="sc-count">${count}</span>
        </button>`;
    }).join('');
}
function goLang(lang){
    LANG_FILTER = lang;
    localStorage.setItem("dl_lang", lang);
    const sel = $("#lang-filter");
    if(sel){ sel.value = lang; sel.disabled = false; }
    renderLangShortcuts();
    CURRENT = "dlstreams";
    document.querySelectorAll(".tab").forEach(t=>t.classList.toggle("active", t.dataset.src==="dlstreams"));
    navigateTo('catalog');
    loadCatalog("dlstreams").then(render);
}

function b64u(s){ return btoa(unescape(encodeURIComponent(s))).replace(/=+$/,"").replace(/\+/g,"-").replace(/\//g,"_"); }
function escapeHtml(s){ return (s||"").replace(/[&<>"']/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c])); }

let _hls = null;
function closePlayer(){
    const video = $("#player-frame");
    video.pause();
    video.src = "";
    if (_hls) { _hls.destroy(); _hls = null; }
    $("#player-modal").classList.remove("active");
}
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
$("#player-close").addEventListener("click", closePlayer);
document.addEventListener("keydown", e=>{ if(e.key==="Escape") closePlayer(); });
$("#player-modal").addEventListener("click", e=>{ if(e.target === e.currentTarget) closePlayer(); });
document.addEventListener("click", (e)=>{
    if(e.target.closest(".channel-item") && e.target.closest(".channel-item").dataset.play){
        e.preventDefault();
        const item = e.target.closest(".channel-item");
        const name = item.querySelector(".name").textContent;
        openPlayer(item.dataset.play, name);
    }
});
document.addEventListener("click", (e)=>{
    const row = e.target.closest(".top-row");
    if(row && row.dataset.play){
        const name = row.querySelector(".top-name").textContent;
        openPlayer(row.dataset.play, name);
    }
});

$("#add-source-btn").addEventListener("click", async ()=>{
    const url = $("#source-url").value.trim();
    const out = $("#add-source-result");
    if(!url){
        out.innerHTML = '<div class="alert alert-error">Veuillez entrer une URL</div>';
        return;
    }
    $("#add-source-btn").disabled = true;
    $("#add-source-btn").textContent = "⏳ Scraping...";
    try{
        const r = await apiFetch(`/api/add-source?url=${encodeURIComponent(url)}`);
        const d = await r.json();
        if(d.success){
            out.innerHTML = `<div class="alert alert-success">${d.message}</div>`;
            $("#source-url").value = "";
            toast(d.message, 'success');
            await loadManualChannels();
            await refreshStats();
        }else{
            out.innerHTML = `<div class="alert alert-error">${d.message}</div>`;
            toast(d.message, 'error');
        }
    }catch(e){
        if (e.message !== 'unauthenticated') {
            out.innerHTML = `<div class="alert alert-error">Erreur: ${e.message}</div>`;
            toast('Erreur: ' + e.message, 'error');
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
    localStorage.setItem("dl_lang", e.target.value);
    renderLangShortcuts();
    loadCatalog(CURRENT);
    render();
});
if($("#lang-filter")) $("#lang-filter").value = LANG_FILTER;
if($("#catalog-sort")) $("#catalog-sort").value = SORT;
document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",async ()=>{
    document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));
    b.classList.add("active");
    CURRENT = b.dataset.src;
    const lf = $("#lang-filter");
    if(lf) lf.disabled = CURRENT === "vavoo";
    if(!ALL[CURRENT].length) await loadCatalog(CURRENT);
    render();
}));

// ---- LOGS live (terminal) ----
let allLogs = [];
let logsInterval = null;
let logsPaused = false;

function restartLogPolling() {
    if (logsInterval) { clearInterval(logsInterval); logsInterval = null; }
    if (logsPaused) return;
    const ms = Number(document.getElementById('logs-interval').value);
    if (ms > 0) logsInterval = setInterval(loadLogs, ms);
}

function toggleLogPause() {
    logsPaused = !logsPaused;
    const btn = document.getElementById('logs-pausebtn');
    const dot = document.getElementById('logs-dot');
    const text = document.getElementById('logs-statustext');
    if (logsPaused) {
        if (logsInterval) { clearInterval(logsInterval); logsInterval = null; }
        btn.textContent = '▶️ Reprendre'; btn.classList.add('paused');
        dot.classList.add('paused'); text.textContent = 'Paused';
    } else {
        btn.textContent = '⏸️ Pause'; btn.classList.remove('paused');
        dot.classList.remove('paused'); text.textContent = 'Live';
        restartLogPolling();
    }
}

async function loadLogs() {
    if (logsPaused) return;
    try {
        const res = await apiFetch('/api/logs');
        if (!res.ok) { return; }
        allLogs = await res.json();
        renderLogs();
        updateLogsBadge();
    } catch (e) { /* silencieux */ }
}

function updateLogsBadge() {
    const badge = document.getElementById('logs-badge');
    if (!badge) return;
    const errs = allLogs.filter(l => l.code >= 400).length;
    if (errs) { badge.style.display = 'inline-block'; badge.textContent = errs; }
    else badge.style.display = 'none';
}

function renderLogs() {
    const methodFilter = document.getElementById('logs-method').value;
    const search = document.getElementById('logs-search').value.trim().toLowerCase();

    let filtered = [...allLogs].reverse(); // du plus ancien au plus récent, défilement vers le bas
    if (methodFilter) filtered = filtered.filter(l => l.method === methodFilter);
    if (search) filtered = filtered.filter(l =>
        String(l.path||'').toLowerCase().includes(search) || String(l.ip||'').toLowerCase().includes(search));

    const term = document.getElementById('logs-term');
    if (!filtered.length) {
        term.innerHTML = '<div class="logs-empty"><div class="icon">📋</div><p>Aucun log ne correspond</p></div>';
        return;
    }
    const wasAtBottom = term.scrollTop + term.clientHeight >= term.scrollHeight - 40;
    term.innerHTML = filtered.slice(0, 300).map(l => {
        const cls = l.code >= 500 ? 'row-err' : (l.code >= 400 ? 'row-warn' : '');
        const codeCls = l.code >= 500 ? 'err' : (l.code >= 400 ? 'warn' : 'ok');
        return `<div class="log-row ${cls}">
            <span class="log-time">${escapeHtml(l.t)}</span>
            <span class="log-method ${escapeHtml(l.method)}">${escapeHtml(l.method)}</span>
            <span class="log-code ${codeCls}">${l.code}</span>
            <span class="log-ip">${escapeHtml(l.ip)}</span>
            <span class="log-path">${escapeHtml(l.path)}</span>
        </div>`;
    }).join('');
    if (wasAtBottom) term.scrollTop = term.scrollHeight;
}

function exportLogs() {
    if (!allLogs.length) { toast('⚠️ Aucun log à exporter', 'warn'); return; }
    const lines = [...allLogs].reverse().map(l => `[${l.t}] ${l.method} ${l.code} ${l.ip} ${l.path}`);
    const blob = new Blob([lines.join('\n')], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'dlstreams-logs.txt';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    toast('✅ Export téléchargé', 'success');
}

async function clearLogs() {
    if (!confirm('Vider tous les logs ? Cette action est irréversible.')) return;
    try {
        const res = await fetch('/api/logs', { method: 'DELETE', headers: { 'Content-Type': 'application/json' } });
        if (res.ok) { toast('✅ Logs vidés', 'success'); await loadLogs(); }
        else toast('❌ Erreur lors du reset', 'error');
    } catch (e) { toast('❌ ' + e.message, 'error'); }
}

// ---- PAGE SYSTEME ----
function fmtBytes(b){
    if(b==null) return "—";
    if(b >= 1073741824) return (b/1073741824).toFixed(1) + " Go";
    if(b >= 1048576) return (b/1048576).toFixed(1) + " Mo";
    if(b >= 1024) return (b/1024).toFixed(1) + " Ko";
    return b + " o";
}
function sysRows(rows){
    return rows.map(([k,v]) => `<div class="sys-row"><div class="sys-key">${escapeHtml(k)}</div><div class="sys-val">${escapeHtml(String(v))}</div></div>`).join('');
}
async function loadSystem(){
    const grid = $("#sys-grid");
    const cache = $("#sys-cache");
    if(!grid) return;
    grid.innerHTML = '<div class="fav-empty">chargement…</div>';
    if(cache) cache.innerHTML = '<div class="fav-empty">chargement…</div>';
    try{
        const r = await apiFetch('/api/system');
        const d = await r.json();
        const mem = d.memory || {};
        const cpu = mem.cpu != null ? ` · CPU ${mem.cpu}%` : "";
        grid.innerHTML = sysRows([
            ['Version', d.version],
            ['Python', d.python],
            ['Port', d.port],
            ['PID', d.pid],
            ['Plateforme', d.platform],
            ['CPU', `${d.cpus} cœur(s)${cpu}`],
            ['Mémoire (processus)', fmtBytes(mem.rss)],
            ['Mémoire système', mem.total ? `${fmtBytes(mem.total - mem.available)} / ${fmtBytes(mem.total)} (${mem.percent}%)` : "n/d"],
            ['Disque', d.disk ? `${fmtBytes(d.disk.used)} / ${fmtBytes(d.disk.total)} (libre ${fmtBytes(d.disk.free)})` : "n/d"],
            ['Démarré le', d.started_at],
            ['Uptime', fmtDur(d.uptime)],
            ['Chaînes totales', d.channels_total],
        ]);
        if(cache){
            cache.innerHTML = sysRows([
                ['Chaînes dlstreams', `${d.cache.dlstreams.count} (maj ${d.cache.dlstreams.age_seconds!=null ? "il y a " + fmtAge(d.cache.dlstreams.age_seconds) : "jamais"})`],
                ['Chaînes Vavoo', `${d.cache.vavoo.count} (maj ${d.cache.vavoo.age_seconds!=null ? "il y a " + fmtAge(d.cache.vavoo.age_seconds) : "jamais"})`],
            ]);
        }
    }catch(e){
        if (e.message !== 'unauthenticated') grid.innerHTML = '<div class="fav-empty">erreur de chargement</div>';
    }
}
function restartServer(){
    if(!confirm('Redémarrer le serveur maintenant ?')) return;
    const btn = $("#restart-btn");
    if(btn) btn.disabled = true;
    fetch('/api/restart', {method: 'POST'}).then(r=>r.json()).then(d=>{
        toast('🔄 ' + (d.message || 'Redémarrage en cours...'), 'warn');
    }).catch(()=>{
        toast('❌ Erreur au redémarrage', 'error');
        if(btn) btn.disabled = false;
    });
}

async function boot(){
    await Promise.all([refreshStats(), loadCatalog("dlstreams"), loadPlays()]);
    render();
    renderFavs();
    loadLogs();
    restartLogPolling();
    setInterval(refreshStats, 30000);
    const hp = location.hash.replace('#', '');
    if (hp && hp !== 'dashboard' && document.getElementById('page-' + hp)) navigateTo(hp);
}

checkSession();
</script>
</body>
</html>"""

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
