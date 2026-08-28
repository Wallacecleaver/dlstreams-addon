#!/usr/bin/env python3
"""dlstreams -> Stremio : mini-addon + proxy autonome avec dashboard complet.
Dashboard avec session persistante, gestion des sources, et navigation SPA.
"""
from __future__ import annotations
import base64
import gzip
import hashlib
import hmac
import json
import logging
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
import xml.sax
import zlib
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

PORT = int(os.environ.get("PORT", "8781"))
_VERSION = "1.13.4"

_PASSWORD_GENERATED = "DASHBOARD_PASSWORD" not in os.environ
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD") or secrets.token_urlsafe(9)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
SITE = "https://dlstreams.st"

# MediaFlow Proxy (Light) — sert les sources DURES (VegetaTv, flux captés au navigateur) qui ont
# besoin d'une session/re-proxy des segments. Configuré via .env (MEDIAFLOW_URL, MEDIAFLOW_PASSWORD).
MEDIAFLOW_URL = os.environ.get("MEDIAFLOW_URL", "").rstrip("/")
MEDIAFLOW_PASSWORD = os.environ.get("MEDIAFLOW_PASSWORD", "")

def _mfp_hls(src_url: str, referer: str = "", ua: str = "") -> str:
    """Emballe une source HLS via MFP Light (/proxy/hls) : re-proxifie les segments avec les bons
    headers. `""` si MFP non configuré. Les lignes contendues ferment la connexion ~26s ; en HLS le
    player re-demande les segments sur des connexions fraîches -> survit au kick (cf. loobox)."""
    if not MEDIAFLOW_URL:
        return ""
    params = {"api_password": MEDIAFLOW_PASSWORD, "d": src_url}
    if ua:
        params["h_user-agent"] = ua
    if referer:
        params["h_referer"] = referer
        params["h_origin"] = referer
    return f"{MEDIAFLOW_URL}/proxy/hls/manifest.m3u8?{urllib.parse.urlencode(params)}"

_CH_TTL = 1800
_START_TIME = time.time()
_request_count = 0
_error_count = 0
_stats_lock = threading.Lock()
_hist: list[list[int]] = []
_request_log: list[dict] = []
_chan_plays: dict[str, int] = {}
_recent_plays: list[dict] = []
_hist_err: list[list[int]] = []
_live: dict[str, float] = {}
_LIVE_WINDOW = 180
_HIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_hist.json")
_HIST_KEEP_MIN = 7 * 24 * 60
_sessions: dict[str, float] = {}
_SESSION_TTL = 24 * 3600
_SESSION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_sessions.json")
_login_attempts: dict[str, list[float]] = {}
_LOGIN_MAX_ATTEMPTS = 6
_LOGIN_WINDOW = 300

_PLAYLISTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_playlists.json")
_playlists: list[dict] = []

# ============================================================
# TOKENS D'ACCÈS ADDON — multi-tokens nommés, hash SHA256 stocké
# ============================================================
_TOKENS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_tokens.json")
_tokens: dict[str, dict] = {}   # token_hash -> {"name": "...", "created": ts, "last_used": ts|null, "revoked": bool}
_TOKEN_TTL_DAYS = 365 * 10  # 10 ans (pas d'expiration auto, seulement révocation manuelle)

def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()

def _token_gen() -> str:
    return secrets.token_urlsafe(32)

def _tokens_load():
    global _tokens
    try:
        if os.path.exists(_TOKENS_FILE):
            with open(_TOKENS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                _tokens = data
    except Exception:
        pass

def _tokens_save():
    try:
        with open(_TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(_tokens, f, ensure_ascii=False, indent=1)
    except Exception:
        pass

def _token_verify(token: str) -> tuple[bool, str | None]:
    """Retourne (valide, token_hash). Valide = existe, pas révoqué."""
    if not token:
        return False, None
    h = _token_hash(token)
    info = _tokens.get(h)
    if not info or info.get("revoked"):
        return False, None
    return True, h

def _token_touch(h: str):
    if h in _tokens:
        _tokens[h]["last_used"] = time.time()
        _tokens_save()
_login_attempts: dict[str, list[float]] = {}
_LOGIN_MAX_ATTEMPTS = 6
_LOGIN_WINDOW = 300

_POPULAR_CHANNELS = [
    {"id": "121", "name": "Canal+ France", "lang": "fr"},
    {"id": "122", "name": "Canal+ Sport", "lang": "fr"},
    {"id": "201", "name": "beIN Sports 1", "lang": "fr"},
    {"id": "202", "name": "beIN Sports 2", "lang": "fr"},
    {"id": "203", "name": "beIN Sports 3", "lang": "fr"},
    {"id": "960", "name": "Ligue 1 McDonald's", "lang": "fr"},
    {"id": "68", "name": "Ligue 1 McDonald's 2", "lang": "fr"},
    {"id": "76", "name": "Ligue 1 McDonald's 3", "lang": "fr"},
    {"id": "970", "name": "DAZN 1", "lang": "fr"},
    {"id": "971", "name": "DAZN 2", "lang": "fr"},
    {"id": "972", "name": "DAZN 3", "lang": "fr"},
    {"id": "973", "name": "DAZN 4", "lang": "fr"},
    {"id": "974", "name": "DAZN 5", "lang": "fr"},
]

_CH_LOGO = {
    "121": "https://static.epg.best/fr/CanalPlus.fr.png",
    "122": "https://static.epg.best/fr/CanalPlusSport.fr.png",
    "201": "https://static.epg.best/fr/BeinSports1.fr.png",
    "202": "https://static.epg.best/fr/BeinSports2.fr.png",
    "203": "https://static.epg.best/fr/BeinSports3.fr.png",
    "960": "https://static.epg.best/fr/Ligue1McDonalds.fr.png",
    "68": "https://static.epg.best/fr/Ligue1McDonalds.fr.png",
    "76": "https://static.epg.best/fr/Ligue1McDonalds.fr.png",
    "970": "https://static.epg.best/fr/DAZN1.fr.png",
    "971": "https://static.epg.best/fr/DAZN2.fr.png",
    "972": "https://static.epg.best/fr/DAZN3.fr.png",
    "973": "https://static.epg.best/fr/DAZN4.fr.png",
    "974": "https://static.epg.best/fr/DAZN5.fr.png",
}

def _norm_name(name: str) -> str:
    import unicodedata, re
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for w in ("hd", "fhd", "uhd", "4k", "hevc", "h264", "h265", "fr", "france", "live", "direct"):
        s = re.sub(rf"\b{w}\b", "", s)
    return s.strip()

_LOGO_BY_NAME: dict[str, str] = {}

def _init_logo_mapping():
    global _LOGO_BY_NAME
    _LOGO_BY_NAME.clear()
    for cid, url in _CH_LOGO.items():
        for ch in _POPULAR_CHANNELS:
            if str(ch.get("id")) == str(cid):
                key = _norm_name(ch["name"])
                if key and url:
                    _LOGO_BY_NAME[key] = url
                break

_init_logo_mapping()

# ============================================================
# LOGOS LOCAUX (dossier LOGOS/<CATEGORIE>/*.png) — curés, rapprochés par nom.
# Sert aussi de source de vérité pour la CATEGORIE (le dossier = le genre).
# ============================================================
_LOGOS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "LOGOS")
_FOLDER2GENRE = {
    "SPORT": "Sports", "CINEMA": "Cinéma", "DOCU": "Documentaire",
    "INFOS": "Actualités", "JEUNESSE": "Jeunesse", "MUSIC": "Musique",
    "GENERAL": "Télévision",
}
# mots de qualité / statut à ignorer dans le rapprochement
_LOGO_NOISE = {"hd", "fhd", "uhd", "4k", "hevc", "h264", "h265", "vip", "mcdonald",
    "mcdonalds", "backup", "event", "events", "only", "during", "live", "direct",
    "tv", "access"}
# tags PAYS : retirés seulement s'ils ne sont pas en 1re position (garder "France 2/3/4/5")
_LOGO_COUNTRY = {"france", "italy", "italia", "poland", "polska", "spain", "espana",
    "greece", "portugal", "germany", "deutschland", "uk", "usa", "international", "gr"}
_LOGO_ALIAS = {"canalfrance": "canal", "canalplus": "canal"}

def _logo_key(name: str) -> str:
    """Clé tolérante de rapprochement nom de chaîne <-> nom de fichier logo.

    Gère les particularités Vavoo (suffixe provider ` |D`/`|E`/`|H`, tags
    `(BACKUP)`/`[EVENT ONLY]`, qualité HD/FHD, tag pays en fin de nom)."""
    import unicodedata, re
    s = name.rsplit(".", 1)[0]
    s = re.sub(r"[\s_-]*logo[\s_-]*$", "", s, flags=re.I)
    s = s.replace("&amp;", "&").split("|")[0]           # suffixe provider Vavoo
    s = re.sub(r"\(.*?\)|\[.*?\]", "", s)               # (BACKUP) [EVENT ONLY]
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    s = s.replace("+", " ")
    parts = [w for w in re.split(r"[^a-z0-9]+", s) if w and w not in _LOGO_NOISE]
    parts = [w for i, w in enumerate(parts) if not (i > 0 and w in _LOGO_COUNTRY)]
    parts = ["sport" if w == "sports" else w for w in parts]
    k = "".join(parts)
    return _LOGO_ALIAS.get(k, k)

_LOCAL_LOGO: dict[str, tuple[str, str]] = {}   # key -> (genre, chemin absolu .png)

def _init_local_logos():
    _LOCAL_LOGO.clear()
    try:
        for cat in os.listdir(_LOGOS_DIR):
            genre = _FOLDER2GENRE.get(cat.upper())
            d = os.path.join(_LOGOS_DIR, cat)
            if not genre or not os.path.isdir(d):
                continue
            for f in os.listdir(d):
                if f.lower().endswith(".png"):
                    k = _logo_key(f)
                    if k and k not in _LOCAL_LOGO:
                        _LOCAL_LOGO[k] = (genre, os.path.join(d, f))
    except Exception:
        pass

_init_local_logos()

_local_logo_lookup_cache: dict[str, tuple[str, str] | None] = {}

def _local_logo_for(name: str):
    """(chemin_png, genre) si un logo curé correspond au nom, sinon None."""
    if not name:
        return None
    if name in _local_logo_lookup_cache:
        return _local_logo_lookup_cache[name]
    hit = _LOCAL_LOGO.get(_logo_key(name))
    res = (hit[1], hit[0]) if hit else None
    if len(_local_logo_lookup_cache) > 4000:
        _local_logo_lookup_cache.clear()
    _local_logo_lookup_cache[name] = res
    return res

_local_logo_bytes_cache: dict[str, bytes] = {}

def _read_local_logo(path: str) -> bytes | None:
    b = _local_logo_bytes_cache.get(path)
    if b is None:
        try:
            with open(path, "rb") as f:
                b = f.read()
            if b[:3] not in (b"\xff\xd8\xff", b"\x89PN"):
                b = None
        except Exception:
            b = None
        if b is not None:
            _local_logo_bytes_cache[path] = b
    return b

_SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_settings.json")
_SETTINGS_DEFAULT = {
    "logos": True,
    "epg": True,
    "epg_url": "https://xmltvfr.fr/xmltv/xmltv.xml.gz",
    "genres": {},
    "stremio": {
        "manifest_name": "",
        "manifest_desc": "",
        "include_dlstreams": True,
        "include_vavoo": True,
        "include_vegetatv": True,
        "default_lang": "fr",
        "channel_names": {},
        "channel_logos": {},
        "channel_streams": {},
        "channel_epg": {},
        "custom_channels": {},
    },
    "wiseplay": {
        "access_code": "",
        "channels": {},
        "playlists": {},
        "sources": {
            "dlstreams": True,
            "vavoo": True,
            "vegetatv": True
        }
    }
}
_settings: dict = dict(_SETTINGS_DEFAULT)

def _settings_save():
    try:
        with open(_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(_settings, f, ensure_ascii=False, indent=1)
    except Exception as e:
        pass

def _settings_load():
    global _settings
    try:
        if os.path.exists(_SETTINGS_FILE):
            with open(_SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                _s = dict(_SETTINGS_DEFAULT)
                for k, v in data.items():
                    if k in _SETTINGS_DEFAULT:
                        if k == "stremio" and isinstance(v, dict):
                            _s[k] = dict(_SETTINGS_DEFAULT[k])
                            _s[k].update(v)
                        else:
                            _s[k] = v
                _settings = _s
    except Exception as e:
        pass

_CH_EPG = {
    "121": "CanalPlus.fr",
    "122": "CanalPlusSport.fr",
    "201": "beINSPORTS1.fr",
    "202": "beINSPORTS2.fr",
    "203": "beINSPORTS3.fr",
    "960": "Ligue1McDonalds.fr",
    "68": "Ligue1McDonalds.fr",
    "76": "Ligue1McDonalds.fr",
    "970": "DAZN1.fr",
    "971": "DAZN2.fr",
    "972": "DAZN3.fr",
    "973": "DAZN4.fr",
    "974": "DAZN5.fr",
}
_EPG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_epg.json")
_EPG_TTL = 6 * 3600
_epg_lock = threading.Lock()
_epg_data: dict[str, list[dict]] = {}
_epg_at: float = 0.0

def _xmlts(ts: str) -> float:
    try:
        y = int(ts[0:4]); mo = int(ts[4:6]); d = int(ts[6:8])
        h = int(ts[8:10]); mi = int(ts[10:12]); s = int(ts[12:14])
        off = ts[15:20] if len(ts) > 14 and " " in ts else ""
        offs = 0
        if off and len(off) == 5:
            oh, om = int(off[1:3]), int(off[3:5])
            offs = (oh * 60 + om) * 60 * (-1 if off[0] == "-" else 1)
        import calendar
        return calendar.timegm((y, mo, d, h, mi, s, 0, 0, 0)) - offs
    except Exception as e:
        return 0

class _EpgHandler(xml.sax.handler.ContentHandler):
    def __init__(self, want: set[str], lo: float, hi: float):
        self.want = want
        self.lo, self.hi = lo, hi
        self.out: dict[str, list[dict]] = {}
        self._chan: str | None = None
        self._prog: list | None = None
        self._title = ""
        self._desc = ""
        self._in_title = False
        self._in_desc = False
    def startElement(self, name, attrs):
        if name == "programme":
            ch = attrs.get("channel", "")
            if ch in self.want:
                st = _xmlts(attrs.get("start", ""))
                sp = _xmlts(attrs.get("stop", ""))
                if sp > self.lo and st < self.hi:
                    self._chan = ch
                    self._prog = [st, sp]
        elif name == "title":
            self._in_title = self._chan is not None
        elif name == "desc":
            self._in_desc = self._chan is not None
    def characters(self, content):
        if self._in_title:
            self._title += content
        elif self._in_desc:
            self._desc += content
    def endElement(self, name):
        if name == "title":
            self._in_title = False
        elif name == "desc":
            self._in_desc = False
        elif name == "programme":
            if self._prog is not None:
                self.out.setdefault(self._chan, []).append({
                    "start": self._prog[0], "stop": self._prog[1],
                    "title": " ".join(self._title.split()),
                    "desc": " ".join(self._desc.split())})
            self._chan = None
            self._prog = None
            self._title = ""
            self._desc = ""

def _epg_save():
    try:
        with open(_EPG_FILE, "w", encoding="utf-8") as f:
            json.dump({"at": _epg_at, "data": _epg_data}, f, ensure_ascii=False)
    except Exception as e:
        pass

def _epg_load():
    global _epg_at
    try:
        if os.path.exists(_EPG_FILE):
            with open(_EPG_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            with _epg_lock:
                _epg_data.clear()
                _epg_data.update(d.get("data", {}))
                _epg_at = float(d.get("at", 0.0))
    except Exception as e:
        pass

def _epg_refresh(force: bool = False):
    global _epg_at
    if not _settings.get("epg", True):
        return
    now = time.time()
    if not force and (_epg_data and now - _epg_at < _EPG_TTL):
        return
    tmp = None
    try:
        url = _settings.get("epg_url") or _SETTINGS_DEFAULT["epg_url"]
        import tempfile
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        body = urllib.request.urlopen(req, timeout=240).read()
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".gz")
        tmp.write(body)
        tmp.close()
        handler = _EpgHandler(set(_CH_EPG.values()), now - 2 * 3600, now + 26 * 3600)
        if body[:2] == b"\x1f\x8b":
            f = gzip.open(tmp.name, "rb")
        else:
            f = open(tmp.name, "rb")
        try:
            xml.sax.parse(f, handler)
        finally:
            f.close()
        with _epg_lock:
            _epg_data.clear()
            _epg_data.update(handler.out)
            _epg_at = time.time()
            _epg_save()
        log.info(f"epg: {len(_epg_data)} chaines guidees ({len(handler.out)} programmes)")
    except Exception as e:
        log.error(f"epg: erreur ({type(e).__name__}: {e})")
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp.name)
            except Exception as e:
                pass

def _epg_slot(dl_id) -> tuple[dict | None, dict | None]:
    xid = _CH_EPG.get(str(dl_id))
    if not xid:
        return None, None
    now = time.time()
    cur = None
    nxt = None
    with _epg_lock:
        for p in _epg_data.get(xid, []):
            if p["start"] <= now < p["stop"]:
                cur = p
            elif p["start"] > now and (nxt is None or p["start"] < nxt["start"]):
                nxt = p
    return cur, nxt

_GENRE_CHOICES = ["Sports", "Actualités", "Films & Séries", "Cinéma", "Divertissement",
    "Musique", "Documentaire", "Jeunesse", "Télévision"]

def _genres_for(name: str) -> list[str]:
    key = name.lower()
    ov = _settings.get("genres", {}).get(key)
    if ov:
        return ov
    return _genre_for(name)

def _detect_lang(name: str) -> str:
    n = name.lower()
    # Marqueurs de région clairement non francophones : ils priment sur les noms de marque
    # (bein sports, canal+...) qui existent aussi dans des déclinaisons internationales.
    foreign_markers = ["mena", "malaysia", "malaisie", "poland", "polska", "czechia", "czech",
        "australia", "australie", "deutschland", "germany", "italia", "espana", "españa", "spain",
        "brazil", "brasil", "nederland", "netherlands", "romania", "bulgaria", "hungary", "magyar",
        "sweden", "norway", "denmark", "finland", "greece", "turkey", "türkiye", "india", "pakistan",
        "english"]
    if any(m in n for m in foreign_markers):
        if any(x in n for x in ["english", "uk", "usa", "espn", "fox", "cnn", "nbc", "sky sports"]):
            return "en"
        if any(x in n for x in ["españa", "espana", "spain", "movistar"]):
            return "es"
        if any(x in n for x in ["deutschland", "germany", " de ", "ard", "zdf"]):
            return "de"
        if any(x in n for x in ["italia", "italy", " it ", "rai"]):
            return "it"
        return "other"
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
    # Catégorie curée (dossier LOGOS/<CAT>/) = source de vérité si la chaîne y figure.
    loc = _local_logo_for(name)
    if loc:
        return [loc[1]]
    n = name.lower().strip()
    if any(k in n for k in ["sport", "foot", "tennis", "racing", "formula", "f1 racing", "golf", "cycl",
        "beinsport", "bein", "eurosport", "rmc sport", "canal+ sport", "ufc", "boxe",
        "mma", "wwe", "équipe", "equipe", "olymp", "auto moto", "ligue 1", "ligue1", "ligue 2", "ligue2"]):
        return ["Sports"]
    if any(k in n for k in ["news", "info", "bfm", "cnews", "france info", "cnn", "bbc", "sky news",
        "al jazeera", "rt ", "euronews", "lcp", "public senat", "parlement"]):
        return ["Actualités"]
    if any(k in n for k in ["kids", "gulli", "cartoon", "piwi", "tiiji", "disney", "nickelodeon",
        "boomerang", "canal j", "junior", "télétoon", "télétoon"]):
        return ["Jeunesse"]
    if any(k in n for k in ["cinema", "cinéma", "cine+", "ciné+", "ocs", "paramount", "action",
        "horror", "polar", "classic", "grand écran", "grand ecran", "premiere"]):
        return ["Cinéma"]
    if n in {"tf1", "france 2", "france 3", "france 4", "france 5", "m6", "tmc", "w9", "arte",
        "c8", "6ter", "canal+ france"}:
        return ["Télévision"]
    if any(k in n for k in ["film", "séries", "series", "family", "série club", "téléfilm", "canal+"]):
        return ["Films & Séries"]
    if any(k in n for k in ["musique", "music", "mtv", "radio", "clip", "melody", "nrj hits", "fun tv"]):
        return ["Musique"]
    if any(k in n for k in ["découverte", "decouverte", "documentaire", "voyage", "histoire",
        "geo", "planète", "planete", "animaux", "nature", "science", "investigation"]):
        return ["Documentaire"]
    if any(k in n for k in ["divertissement", "télé réalité", "télé-realite", "w9", "tfx",
        "chérie", "cherie", "cstar", "nrj 12", "seduction"]):
        return ["Divertissement"]
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
    global _sessions
    try:
        if os.path.exists(_SESSION_FILE):
            with open(_SESSION_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            now = time.time()
            _sessions = {k: float(v) for k, v in data.items()
                if isinstance(v, (int, float)) and (now - float(v)) < _SESSION_TTL}
    except Exception as e:
        pass

def _sessions_save():
    try:
        with open(_SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(_sessions, f)
    except Exception as e:
        pass

def _playlists_load():
    global _playlists
    try:
        if os.path.exists(_PLAYLISTS_FILE):
            with open(_PLAYLISTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                _playlists = data
    except Exception as e:
        pass

def _playlists_save():
    try:
        with open(_PLAYLISTS_FILE, "w", encoding="utf-8") as f:
            json.dump(_playlists, f, ensure_ascii=False, indent=1)
    except Exception as e:
        pass

def _hist_load():
    global _hist, _hist_err
    try:
        if os.path.exists(_HIST_FILE):
            with open(_HIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            keep_from = time.time() // 60 - _HIST_KEEP_MIN
            if isinstance(data, dict):
                _hist = [[int(m), int(c)] for m, c in data.get("req", [])
                    if isinstance(m, (int, float)) and isinstance(c, (int, float)) and m >= keep_from]
                _hist_err = [[int(m), int(c)] for m, c in data.get("err", [])
                    if isinstance(m, (int, float)) and isinstance(c, (int, float)) and m >= keep_from]
            else:
                _hist = [[int(m), int(c)] for m, c in data
                    if isinstance(m, (int, float)) and isinstance(c, (int, float)) and m >= keep_from]
    except Exception as e:
        pass

def _hist_save():
    try:
        with open(_HIST_FILE, "w", encoding="utf-8") as f:
            json.dump({"req": _hist, "err": _hist_err}, f)
    except Exception as e:
        pass

def _track_play(src: str, cid: str):
    key = f"{src}:{cid}"
    now = time.time()
    with _stats_lock:
        _chan_plays[key] = _chan_plays.get(key, 0) + 1
        _recent_plays.append({"src": src, "cid": cid, "t": now})
        if len(_recent_plays) > 40:
            del _recent_plays[:len(_recent_plays) - 40]
        _live[key] = now
        if len(_live) > 200:
            cutoff = now - _LIVE_WINDOW
            for k in [k for k, t in _live.items() if t < cutoff]:
                del _live[k]

def _live_plays() -> list[dict]:
    now = time.time()
    with _stats_lock:
        items = [(k, t) for k, t in _live.items() if now - t <= _LIVE_WINDOW]
        items.sort(key=lambda kv: -kv[1])
        out = []
        for key, t in items:
            src, _, cid = key.partition(":")
            out.append({"key": key, "src": src, "id": cid, "name": _name_for(src, cid), "last": t})
        return out

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
    except Exception as e:
        pass
    mem: dict = {}
    try:
        import psutil
        vm = psutil.virtual_memory()
        proc = psutil.Process(os.getpid())
        mem = {"total": vm.total, "used": vm.used, "percent": vm.percent,
            "rss": proc.memory_info().rss, "cpu": proc.cpu_percent(interval=0.2)}
    except Exception as e:
        try:
            import resource
            mem = {"rss": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}
        except Exception as e:
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
        except Exception as e:
            pass
        _ch_cache.update(at=now, list=list(seen.values()))
    for ch in _ch_cache["list"]:
        lg = _CH_LOGO.get(str(ch.get("id")))
        if lg:
            ch["logo"] = lg
        key = _norm_name(ch.get("name", ""))
        if key and key not in _LOGO_BY_NAME:
            _LOGO_BY_NAME[key] = lg
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
        return [], f" Erreur: {type(e).__name__}: {e}"

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
    except Exception as e:
        w = ""
    pairs = re.findall(r'data-url="([^"]+)"[^>]*title="([^"]*)"', w)
    out = [(title.strip() or f"Player {i + 1}", url)
        for i, (url, title) in enumerate(pairs) if url.startswith("http")]
    if out:
        path_names = {
            "stream": "Stream principal",
            "watch": "Watch",
            "player": "Player",
            "plus": "Plus",
            "hub": "Hub",
            "cast": "Cast",
            "casting": "Casting",
        }
        for i, (title, url) in enumerate(out):
            if re.match(r'^player\s+\d+$', title, re.IGNORECASE):
                path = _PLAYER_PATHS[i] if i < len(_PLAYER_PATHS) else "player"
                out[i] = (path_names.get(path, path.capitalize()), url)
        return out
    path_names = {
        "stream": "Stream principal",
        "watch": "Watch",
        "player": "Player",
        "plus": "Plus",
        "hub": "Hub",
        "cast": "Cast",
        "casting": "Casting",
    }
    return [(path_names.get(p, p.capitalize()), f"{SITE}/{p}/stream-{cid}.php")
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
        except Exception as e:
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
        except Exception as e:
            continue
    raise ValueError("aucun player ne resout")

def working_players(cid: str) -> list[tuple[int, str]]:
    pls = players(cid)
    def _chk(item: tuple[int, tuple[str, str]]) -> tuple[int, str] | None:
        i, (label, url) = item
        try:
            resolve_player(url)
            return (i, label)
        except Exception as e:
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
        except Exception as e:
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
            except Exception as e:
                break
            _vavoo_base["url"] = base
            return d
    return None

def vavoo_channels(country: str = "France") -> list[dict]:
    if _vavoo_cache["list"] and time.time() - _vavoo_cache["at"] < 6 * 3600:
        return [c for c in _vavoo_cache["list"] if c.get("lang") == country.lower() or country == "France"]
    items, cursor, pages = [], 0, 0
    while cursor is not None and pages < 40:
        d = _vavoo_post("catalog", {"language": "fr", "region": "FR", "catalogId": "iptv", "id": "",
            "adult": False, "search": "", "sort": "name",
            "cursor": cursor, "clientVersion": "3.1.0"})
        if not d:
            break
        batch = d.get("items") or []
        if not batch:
            break
        for x in batch:
            url = x.get("url")
            if not url:
                continue
            name = x.get("name") or ""
            logo = x.get("logo") or ""
            lang = _detect_lang(name)
            if country == "France" and lang != "fr":
                continue
            items.append({"id": url, "name": name, "logo": logo, "lang": lang})
            if logo and name:
                key = _norm_name(name)
                if key and key not in _LOGO_BY_NAME:
                    _LOGO_BY_NAME[key] = logo
        cursor, pages = d.get("nextCursor"), pages + 1
    if items:
        _vavoo_cache.update(at=time.time(), list=items)
    return [c for c in items if c.get("lang") == "fr"]

def vavoo_resolve(vurl: str) -> str:
    if not vurl:
        return ""
    d = _vavoo_post("resolve", {"language": "fr", "region": "FR", "url": str(vurl), "clientVersion": "3.0.2"})
    if isinstance(d, list) and d:
        return d[0].get("url") or d[0].get("streamUrl") or ""
    return ""

# ============================================================
# VEGETATV — source live via spool distant health-checké (server_status.json) + registre
# chaîne->serveurs + resolve() load-balancé byte-testé, servi via MFP. Portage stdlib de
# app/vegetatv.py de loobox. AUCUN lien avec dlstreams/vavoo (source à part).
# ============================================================
_VEGETA_SPOOL = "http://vegetatv.duckdns.org/data/server_status.json"
_VEGETA_UA = "Lavf/60"
_VEGETA_SPOOL_TTL = 120
_VEGETA_REG_TTL = 45 * 60
_VEGETA_MAX_SERVERS = 15      # serveurs `up` (les + rapides) ingérés — cap RAM/temps
_VEGETA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_vegetatv.json")
_VEGETA_FR = re.compile(r"\b(FR|FRA|FRANCE|FRENCH|FRANCAIS|FRANÇAIS)\b", re.I)
# Préfixe pays étranger : le code (CA/UK/BE…) suivi d'un séparateur — QUI VARIE selon la ligne :
# « | », « : », « ▎ », « ‖ », « ▐ », « ┃ », ou un simple espace. loobox n'attendait que « | ».
_VEGETA_SEP_CH = "|:▎‖▐┃"
_VEGETA_NOTFR = re.compile(
    r"^\s*(?:CA|AR|US|UK|BE|CH|DE|ES|IT|PT|NL|MA|DZ|TN|TR|QC|CD|SN|CI)(?:[\-\s]*FR)?\s*[" + _VEGETA_SEP_CH + r"]"
    r"|\b(?:QUEBEC|QUÉBEC|CANADA|CANADIAN|ARABIC|ARABE|ONTARIO|MONTREAL)\b", re.I)
_VEGETA_PREFIX = re.compile(r"^\s*[A-Za-z0-9]{1,4}\s*[" + _VEGETA_SEP_CH + r"]\s*")
_VEGETA_SEP = re.compile(r"#{2,}|={3,}|\*{3,}|▬{2,}")

def _vegeta_clean_name(name: str) -> str:
    """Retire le préfixe fournisseur/pays (« FR| », « BE ▎ », « UK: ») pour un affichage propre."""
    return _VEGETA_PREFIX.sub("", name or "").strip()
_vegeta_spool_cache: dict = {"at": 0.0, "servers": []}
_vegeta_reg: dict = {"at": 0.0, "reg": {}}

def _vegeta_json(url: str, timeout: int = 20):
    req = urllib.request.Request(url, headers={"User-Agent": _VEGETA_UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))

def _vegeta_chkey(name: str) -> str:
    """Clé canonique d'une chaîne (retire le préfixe fournisseur « XX| », normalise, dé-pluralise).
    Regroupe la même chaîne à travers les serveurs et sert d'id Stremio."""
    s = re.sub(r"^[^|]{1,15}\|\s*", "", name or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().replace("+", " plus ")
    s = re.sub(r"\(\d+\)|\[[^\]]*\]", " ", s)
    out = []
    for w in re.split(r"[^a-z0-9]+", s):
        if not w:
            continue
        if len(w) > 3 and w.endswith("s"):
            w = w[:-1]
        out.append(w)
    return " ".join(out)

def _vegeta_is_fr(name: str, group: str) -> bool:
    txt = f"{name} {group}"
    if _VEGETA_NOTFR.search(txt):
        return False
    return bool(_VEGETA_FR.search(txt))

def _vegeta_parse_spool(data: dict) -> list:
    out = []
    for raw_url, s in (data.get("servers") or {}).items():
        if s.get("kind") != "xtream":
            continue
        u = s.get("url") or raw_url
        m = re.match(r"(https?://[^/]+)/get\.php\?(.*)", u)
        if not m:
            continue
        base = m.group(1)
        params = dict(urllib.parse.parse_qsl(m.group(2)))
        usr, pw = params.get("username"), params.get("password")
        if not (usr and pw):
            continue
        out.append({"key": f"{base}|{usr}", "base": base, "username": usr, "password": pw,
                    "rtt": int(s.get("response_time_ms") or 999999), "up": bool(s.get("up"))})
    return out

def _vegeta_spool(force: bool = False) -> list:
    if (not force and _vegeta_spool_cache["servers"]
            and time.time() - _vegeta_spool_cache["at"] < _VEGETA_SPOOL_TTL):
        return _vegeta_spool_cache["servers"]
    try:
        servers = _vegeta_parse_spool(_vegeta_json(_VEGETA_SPOOL))
        if servers:
            _vegeta_spool_cache.update(at=time.time(), servers=servers)
    except Exception:
        pass
    return _vegeta_spool_cache["servers"]

def _vegeta_api_channels(server: dict) -> list:
    """Live FR d'un serveur via player_api (get_live_categories + get_live_streams)."""
    base, u, p = server["base"], server["username"], server["password"]
    api = f"{base}/player_api.php?username={urllib.parse.quote(u)}&password={urllib.parse.quote(p)}"
    try:
        cats = _vegeta_json(api + "&action=get_live_categories", timeout=30)
        catmap = {str(c.get("category_id")): c.get("category_name", "") for c in cats}
        streams = _vegeta_json(api + "&action=get_live_streams", timeout=60)
    except Exception:
        return []
    out = []
    for s in streams if isinstance(streams, list) else []:
        sid = s.get("stream_id")
        raw = (s.get("name") or "").strip()
        if sid is None or not raw or _VEGETA_SEP.search(raw):
            continue
        group = catmap.get(str(s.get("category_id")), "")
        if not _vegeta_is_fr(raw, group):     # filtre sur le nom BRUT (préfixe + group taggés FR)
            continue
        name = _vegeta_clean_name(raw) or raw  # nettoie le préfixe pour l'affichage ET le regroupement
        out.append({"name": name, "logo": s.get("stream_icon") or "", "sid": str(sid)})
    return out

_vegeta_diag: dict = {"at": 0.0, "spool": 0, "up": 0, "tried": 0, "fr_servers": 0, "channels": 0, "err": ""}

def _vegeta_ingest() -> int:
    """Reconstruit le registre chaîne->serveurs depuis les serveurs `up` les + rapides. -> nb chaînes.
    Jamais écrasé par du vide (échec réseau -> ancien registre conservé). Verbeux (docker logs)."""
    spool = _vegeta_spool(force=True)
    up = sorted([s for s in spool if s["up"]], key=lambda s: s["rtt"])
    log.info(f"vegetatv: ingestion — spool {len(spool)} serveurs, {len(up)} up")
    if not spool:
        _vegeta_diag.update(at=time.time(), spool=0, up=0, tried=0, fr_servers=0, channels=0,
            err="spool injoignable depuis le conteneur (DNS/réseau vers vegetatv.duckdns.org ?)")
        log.error("vegetatv: spool injoignable — vérifie la sortie réseau du conteneur")
        return len(_vegeta_load_reg())
    reg: dict = {}
    tried = fr_servers = 0
    for s in up[:_VEGETA_MAX_SERVERS]:
        tried += 1
        chans = _vegeta_api_channels(s)
        if chans:
            fr_servers += 1
        for c in chans:
            k = _vegeta_chkey(c["name"])
            if not k:
                continue
            e = reg.setdefault(k, {"display": c["name"], "logo": c["logo"], "refs": []})
            e["refs"].append({"key": s["key"], "sid": c["sid"]})
    if reg:
        _vegeta_reg.update(at=time.time(), reg=reg)
        try:
            with open(_VEGETA_FILE, "w", encoding="utf-8") as f:
                json.dump({"at": time.time(), "reg": reg}, f, ensure_ascii=False)
        except Exception:
            pass
    _vegeta_diag.update(at=time.time(), spool=len(spool), up=len(up), tried=tried,
        fr_servers=fr_servers, channels=len(reg),
        err="" if reg else "0 chaîne FR sur les serveurs testés (aucun serveur FR dans le top rtt ?)")
    log.info(f"vegetatv: registre {len(reg)} chaines ({fr_servers}/{tried} serveurs ont livré du FR)")
    return len(reg)

def _vegeta_load_reg() -> dict:
    if _vegeta_reg["reg"] and time.time() - _vegeta_reg["at"] < _VEGETA_REG_TTL:
        return _vegeta_reg["reg"]
    try:
        with open(_VEGETA_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d.get("reg"), dict):
            _vegeta_reg.update(at=d.get("at", 0.0), reg=d["reg"])
    except Exception:
        pass
    return _vegeta_reg["reg"]

def vegetatv_channels(country: str = "France") -> list:
    reg = _vegeta_load_reg()
    return [{"id": k, "name": v.get("display", ""), "logo": v.get("logo", ""), "lang": "fr"}
            for k, v in reg.items()]

def _vegeta_ts(server: dict, sid: str) -> str:
    return f"{server['base']}/live/{server['username']}/{server['password']}/{sid}.ts"

def _vegeta_m3u8(server: dict, sid: str) -> str:
    return f"{server['base']}/live/{server['username']}/{server['password']}/{sid}.m3u8"

def _vegeta_delivers(url: str, secs: float = 4, cap: int = 200000) -> bool:
    """Le flux crache-t-il vraiment ? Tire <= cap octets, True si > 50 Ko (écarte un 407/0 octet)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _VEGETA_UA})
        got, t = 0, time.time()
        with urllib.request.urlopen(req, timeout=10) as r:
            if getattr(r, "status", 200) != 200:
                return False
            while got <= cap and time.time() - t <= secs:
                chunk = r.read(65536)
                if not chunk:
                    break
                got += len(chunk)
        return got > 50000
    except Exception:
        return False

def _vegeta_pick(name_key: str):
    """Load-balancer byte-testé : refs registre ∩ spool `up`, tri rtt, byte-test des meilleurs.
    On teste jusqu'à 6 serveurs (lignes contendues : si les 3 premiers sont saturés au byte-test,
    un des suivants peut être libre) -> beaucoup moins de lectures « flux introuvable »."""
    e = _vegeta_load_reg().get(name_key)
    if not e:
        return None
    up = {s["key"]: s for s in _vegeta_spool() if s["up"]}
    cands = [(up[r["key"]], r["sid"]) for r in e.get("refs", []) if r["key"] in up]
    cands.sort(key=lambda c: c[0]["rtt"])
    for server, sid in cands[:6]:
        if _vegeta_delivers(_vegeta_ts(server, sid)):
            return server, sid
    # Aucun serveur ne passe le byte-test DIRECT (pull .ts brut depuis l'addon) : ça ne veut PAS dire
    # injouable via MFP (qui maintient la session/reconnexion). On sert quand même le + rapide -> MFP
    # tente ; le client voit un flux ou un échec propre, plutôt qu'un 502 systématique.
    return cands[0] if cands else None

def vegetatv_resolve(name_key: str) -> str:
    """LECTURE : URL MFP HLS du 1er serveur qui livre. `""` si pas de MFP / tout KO / absente."""
    if not MEDIAFLOW_URL:
        return ""
    picked = _vegeta_pick(name_key)
    if not picked:
        return ""
    server, sid = picked
    return _mfp_hls(_vegeta_m3u8(server, sid), ua=_VEGETA_UA)

def _vegeta_warm():
    """Thread de fond : ingestion initiale (registre vide/stale) puis rafraîchi chaque TTL."""
    log.info("vegetatv: thread de fond démarré (MFP configuré)")
    while True:
        try:
            _vegeta_load_reg()
            if not _vegeta_reg["reg"] or time.time() - _vegeta_reg["at"] >= _VEGETA_REG_TTL:
                n = _vegeta_ingest()
                if n:
                    _unified_invalidate()   # -> les chaînes Vegeta apparaissent SANS clic manuel
        except Exception as e:
            log.error(f"vegetatv warm: {e}")
        time.sleep(_VEGETA_REG_TTL)

# ============================================================
# FLUX STREMIO — détection de qualité (par le nom) + format unifié « beau et propre ».
# ============================================================
_QUALITY_RE = [
    (re.compile(r"\b(4k|uhd|2160p?|8k|ultra\s*hd)\b", re.I), ("4K", 4)),
    (re.compile(r"\b(fhd|1080p?|full\s*hd)\b", re.I), ("FHD", 3)),
    (re.compile(r"\b(hd|720p?)\b", re.I), ("HD", 2)),
    (re.compile(r"\b(sd|480p?|360p?|ld)\b", re.I), ("SD", 1)),
]

def _quality_of(name: str) -> tuple[str, int]:
    """Détecte la qualité depuis un nom de chaîne/flux (4K/FHD/HD/SD). ('', 0) si inconnue.
    Instantané (aucun réseau) : la plupart des lignes taguent la qualité dans le nom."""
    for rx, res in _QUALITY_RE:
        if rx.search(name or ""):
            return res
    return "", 0

def _stream_entry(emoji: str, provider: str, quality: str, detail: str, url: str,
                  binge: str = "") -> dict:
    """Objet stream Stremio cohérent : badge « provider + qualité » (2 lignes, style Torrentio)
    + ligne de détail lisible. `bingeGroup` : Stremio garde la même source d'une fois sur l'autre."""
    name = f"{emoji} {provider}".strip()
    if quality:
        name += f"\n{quality}"
    s = {"name": name, "title": detail, "url": url}
    if binge:
        s["behaviorHints"] = {"bingeGroup": binge}
    return s

# ============================================================
# UNIFICATION (modèle tvmio) — fusionne dlstreams + Vavoo + VegetaTv en UN registre de
# chaînes canoniques (par nom normalisé). 1 chaîne = tous les flux (toutes sources) au clic,
# rangées par catégorie. Les variantes de qualité deviennent des flux, pas des chaînes.
# ============================================================
_CANON_NOISE = {"hd", "fhd", "uhd", "4k", "8k", "sd", "720", "1080", "2160", "720p", "1080p",
    "2160p", "hevc", "h264", "h265", "vip", "raw", "local", "backup", "event", "events", "only",
    "during", "live", "direct", "access", "mcdonald", "mcdonalds", "tv", "s1", "s2", "s3", "ld", "rec"}
_CANON_COUNTRY = {"fr", "france", "french", "italy", "italia", "poland", "polska", "spain", "espana",
    "greece", "portugal", "germany", "deutschland", "uk", "usa", "international", "gr", "be",
    "ca", "us", "ar"}

_CANON_MERGE = {}
for _i in range(1, 20):
    _CANON_MERGE[f"beinmax{_i}"] = f"beinsportmax{_i}"        # « beIN MAX 5 » = « beIN Sports Max 5 »
    _CANON_MERGE[f"beinsportfrench{_i}"] = f"beinsport{_i}"   # « beIN Sport French 1 » = « beIN Sports 1 »

def _canon_key(name: str) -> str:
    """Clé canonique de FUSION cross-source (agressive : retire qualité/provider/pays/statut/«+»,
    garde marque + numéro). Le même « beIN Sports 1 » de 3 sources -> même clé."""
    s = re.sub(r"^[^|]{1,15}\|\s*", "", name or "")   # préfixe fournisseur « FR| » « C+FR| » -> retiré
    s = re.sub(r"\s*\|.*$", "", s)                     # suffixe provider Vavoo « |D » « |E » -> retiré
    s = re.sub(r"\(.*?\)|\[.*?\]", "", s)
    # tags qualité en unicode fantaisie (vegeta : « 4ᴋ », « ᴴᴰ », « ᴿᴬᵂ », « ◉ rec »). NFKD décompose
    # ᴴᴰᴿᴬᵂ→HD/RAW (→ ensuite retirés par _CANON_NOISE), MAIS PAS ᴋ (small-cap K) -> on le mappe à la
    # main. SURTOUT ne pas retirer les chiffres attachés (sinon « Ligue 1+ 1ᴿᴬᵂ » perd son « 1 »).
    s = s.replace("ᴋ", "k").replace("◉", "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    s = s.replace("'", "").replace("+", " ")   # « McDonald's » -> « mcdonalds » (sinon le 's traîne)
    parts = [w for w in re.split(r"[^a-z0-9]+", s) if w and w not in _CANON_NOISE]
    parts = [w for i, w in enumerate(parts) if not (i > 0 and w in _CANON_COUNTRY)]
    if parts and parts[0] == "fr":         # préfixe « FR … » en tête -> retiré (garde « France 2 »)
        parts = parts[1:]
    parts = ["sport" if w == "sports" else w for w in parts]
    k = "".join(parts)
    return _CANON_MERGE.get(k, k)

_DISPLAY_ALIAS = {"ligue1": "Ligue 1+", "canal": "Canal+", "canalsport": "Canal+ Sport",
    "canalfoot": "Canal+ Foot", "canalsport360": "Canal+ Sport 360",
    "canalpremierleague": "Canal+ Premier League", "canalcinema": "Canal+ Cinéma",
    "canalgrandecran": "Canal+ Grand Écran", "canalseries": "Canal+ Séries",
    "canalkids": "Canal+ Kids", "canalj": "Canal J", "canaldocs": "Canal+ Docs",
    "canalboxoffice": "Canal+ Box Office", "eurosport1": "Eurosport 1", "eurosport2": "Eurosport 2"}
for _i in range(1, 20):
    _DISPLAY_ALIAS[f"ligue1{_i}"] = f"Ligue 1+ {_i}"
    _DISPLAY_ALIAS[f"beinsport{_i}"] = f"beIN Sports {_i}"
    _DISPLAY_ALIAS[f"beinsportmax{_i}"] = f"beIN Sports Max {_i}"
    _DISPLAY_ALIAS[f"rmcsport{_i}"] = f"RMC Sport {_i}"
    _DISPLAY_ALIAS[f"dazn{_i}"] = f"DAZN {_i}"

_CANON_TAGS = re.compile(
    r"\b(HD|FHD|UHD|4K|8K|SD|720p?|1080p?|2160p?|HEVC|H26[45]|VIP|RAW|LOCAL|BACKUP|EVENT|ONLY|ACCESS|S[123])\b",
    re.I)

def _clean_display_raw(name: str) -> str:
    """Nom d'affichage PROPRE : retire préfixe « FR| », suffixe « |D », (BACKUP)/[EVENT], tags qualité
    fantaisie (« 4ᴋ », « ᴴᴰ », « ◉ rec ») et ASCII (HD/FHD/4K…)."""
    s = re.sub(r"^[^|]{1,15}\|\s*", "", name or "")   # préfixe fournisseur « FR| »
    s = re.sub(r"\s*\|.*$", "", s)                     # suffixe provider « |D »
    s = re.sub(r"\(.*?\)|\[.*?\]", "", s)
    s = s.replace("ᴋ", "k").replace("◉", "")
    s = re.sub(r"[ᴀ-ᵿ⁰-₟]", "", s)                    # tags small-cap/exposant (ᴴᴰ, ᴿᴬᵂ…)
    s = _CANON_TAGS.sub("", s)                         # HD/FHD/4K/RAW/… ASCII
    s = re.sub(r"\brec\b", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -·|▎‖▐┃")
    return s or (name or "").strip()

_FOREIGN_RE = re.compile(
    r"^\s*(?:AFR|AF|USA?|GBR?|UK|ENG|AUS|NZL?|IRL?|CA|AR|BE|CH|DE|ES|IT|PT|NL|MA|DZ|TN|TR|QC|CD|SN|CI|"
    r"PL|POL|RO|RU|BR|SA|EG|LB|IQ|GR|HR|SRB?|SI|SK|CZ|HU|BG|UA)\b[\s\-]*(?:FR)?\s*[|:]"
    r"|\b(?:AUSTRALIA|AUSTRALIE|SERBIA|SERBIE|CROATIA|CROATIE|POLAND|POLSKA|ROMANIA|GREECE|GRECE|"
    r"QUEBEC|QUÉBEC|CANADA|CANADIAN|ARABIC|ARABE|AFRICA|AFRIQUE|BELGIUM|GERMANY|ITALIA|ITALY|SPAIN|"
    r"ESPANA|PORTUGAL|TURKEY|BRAZIL|RUSSIA)\b", re.I)

def _is_foreign(name: str) -> bool:
    return bool(_FOREIGN_RE.search(name or ""))

_SRC_RANK = {"dlstreams": 3, "vavoo": 2, "vegetatv": 1}   # priorité du nom d'affichage
_CAT_ICON = {"Sports": "⚽", "Actualités": "📰", "Films & Séries": "🎬", "Cinéma": "🎥",
    "Divertissement": "🎉", "Musique": "🎵", "Documentaire": "🌍", "Jeunesse": "🧸",
    "Télévision": "📺"}
# slugs ASCII pour les ids de catalogue (évite l'encodage % des accents dans l'URL Stremio)
_CAT_SLUG = {"Sports": "sports", "Actualités": "actualites", "Films & Séries": "films-series",
    "Cinéma": "cinema", "Divertissement": "divertissement", "Musique": "musique",
    "Documentaire": "documentaire", "Jeunesse": "jeunesse", "Télévision": "television"}
_SLUG_CAT = {v: k for k, v in _CAT_SLUG.items()}
_unified_cache: dict = {"at": 0.0, "reg": None}
_UNIFIED_TTL = 1800

def _unified_registry() -> dict:
    """Registre canonique {key -> {name, cat, logo, refs:[{src,id,q,qr}]}}, fusionné + caché."""
    if _unified_cache["reg"] is not None and time.time() - _unified_cache["at"] < _UNIFIED_TTL:
        return _unified_cache["reg"]
    reg: dict = {}

    def add(src, cid, raw):
        key = _canon_key(raw)
        if not key:                       # canon vide (nom bizarre) -> NE JAMAIS dropper la chaîne :
            key = _tvlogos_slug(raw)      # clé de repli depuis le nom brut (sinon perte silencieuse)
        if not key:
            return
        q, qr = _quality_of(raw)
        e = reg.get(key)
        if e is None:
            e = reg[key] = {"key": key, "name": "", "refs": [], "_nr": -1}
        e["refs"].append({"src": src, "id": str(cid), "q": q, "qr": qr})
        rank = _SRC_RANK.get(src, 0)
        if key in _DISPLAY_ALIAS:
            e["name"] = _DISPLAY_ALIAS[key]
            e["_nr"] = 99
        elif rank > e["_nr"]:
            e["name"] = _clean_display_raw(raw)
            e["_nr"] = rank

    for c in channels():
        if c.get("lang") != "fr" or _is_foreign(c["name"]):   # dlstreams porte tout l'international
            continue
        add("dlstreams", c["id"], c["name"])
    for c in vavoo_channels():        # déjà filtré FR, + garde-fou anti-étranger
        if not _is_foreign(c["name"]):
            add("vavoo", c["id"], c["name"])
    if MEDIAFLOW_URL:
        for c in vegetatv_channels():
            if not _is_foreign(c["name"]):
                add("vegetatv", c["id"], c["name"])
    st = _settings.get("stremio", {})
    ov_names = st.get("channel_names", {})
    for e in reg.values():
        if ov_names.get(e["key"]):            # override de nom (édition dashboard)
            e["name"] = ov_names[e["key"]]
        e["cat"] = _genre_for(e["name"])[0]
        seen = set()
        refs = []
        for r in sorted(e["refs"], key=lambda r: -r["qr"]):   # dédup par (source, qualité)
            sig = (r["src"], r["q"])
            if sig in seen:
                continue
            seen.add(sig)
            refs.append(r)
        e["refs"] = refs
    _unified_cache.update(at=time.time(), reg=reg)
    return reg

def _unified_invalidate():
    _unified_cache.update(at=0.0, reg=None)

def _unified_by_cat(cat: str) -> list:
    out = [e for e in _unified_registry().values() if e["cat"] == cat]
    out.sort(key=lambda e: e["name"].lower())
    return out

def unified_categories() -> list:
    """Catégories NON VIDES, dans l'ordre de _GENRE_CHOICES (pour les catalogues Stremio)."""
    present = {e["cat"] for e in _unified_registry().values()}
    return [c for c in _GENRE_CHOICES if c in present]

def unified_streams(key: str, base: str) -> list:
    """Tous les flux d'une chaîne unifiée (toutes sources), badge source+qualité, meilleur d'abord.
    URLs paresseuses (résolution au PLAY via /hls,/vhls,/vghls) -> réponse instantanée."""
    e = _unified_registry().get(key)
    if not e:
        return []
    scored = []
    # Flux PERSO ajoutés depuis le dashboard (channel_streams[key]) -> en tête (rang max).
    for st in _settings.get("stremio", {}).get("channel_streams", {}).get(key, []):
        surl = st.get("url") if isinstance(st, dict) else str(st)
        if not surl:
            continue
        label = (st.get("label") if isinstance(st, dict) else "") or "Flux perso"
        q, qr = _quality_of(label)
        scored.append((100 + qr, _stream_entry("⭐", "Perso", q, label, surl, binge=f"u-{key}")))
    for r in e["refs"]:
        src, q = r["src"], r["q"]
        if src == "dlstreams":
            emoji, prov, url = "🔀", "dlstreams", f"{base}/hls/{r['id']}/index.m3u8"
        elif src == "vavoo":
            emoji, prov, url = "📺", "Vavoo", f"{base}/vhls?v={_b64u(r['id'])}"
        elif src == "vegetatv":
            emoji, prov, url = "🐉", "Vegeta TV", f"{base}/vghls?v={_b64u(r['id'])}"
        else:
            continue
        detail = prov + (f" · {q}" if q else "")
        scored.append((r["qr"], _stream_entry(emoji, prov, q, detail, url, binge=f"u-{key}")))
    scored.sort(key=lambda t: -t[0])
    return [s for _, s in scored]

def _b64u(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")

def _unb64u(s: str) -> str:
    padding = 4 - (len(s) % 4) if len(s) % 4 else 0
    return base64.urlsafe_b64decode(s + "=" * padding).decode()

def _decode_config(config_b64: str) -> dict:
    try:
        json_str = _unb64u(config_b64)
        return json.loads(json_str)
    except Exception:
        return {}

def _extract_config_from_path(path: str) -> tuple[dict, str]:
    if path.startswith("/") and "/" in path[1:]:
        first, rest = path[1:].split("/", 1)
        # Only treat as config if it decodes to valid JSON with expected keys
        config = _decode_config(first)
        if config and isinstance(config, dict) and any(k in config for k in ("pseudo", "device", "epg", "vavoo", "dlstreams", "logos", "quality", "lang", "adult")):
            return config, "/" + rest
    return {}, path

_PROXY_SECRET_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_proxy_secret.key")

def _load_proxy_secret():
    try:
        if os.path.exists(_PROXY_SECRET_FILE):
            with open(_PROXY_SECRET_FILE, "rb") as f:
                secret = f.read()
            if len(secret) == 32:
                return secret
    except Exception:
        pass
    return secrets.token_bytes(32)

def _save_proxy_secret(secret: bytes):
    try:
        with open(_PROXY_SECRET_FILE, "wb") as f:
            f.write(secret)
    except Exception:
        pass

_PROXY_SECRET = _load_proxy_secret()
_save_proxy_secret(_PROXY_SECRET)

def _proxy_sign(u_b64: str, h_b64: str) -> str:
    return hmac.new(_PROXY_SECRET, (u_b64 + "|" + h_b64).encode(), hashlib.sha256).hexdigest()[:24]

def _proxy_ok(u_b64: str, h_b64: str, sig: str) -> bool:
    return bool(sig) and hmac.compare_digest(sig, _proxy_sign(u_b64, h_b64))

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
        ub = _b64u(absu)
        out.append(f"{self_base}/{route}?u={ub}&h={hdr_enc}&s={_proxy_sign(ub, hdr_enc)}")
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
        "vegetatv": {
            "count": len(_vegeta_reg.get("reg") or {}) if MEDIAFLOW_URL else 0,
            "age_seconds": int(time.time() - _vegeta_reg.get("at", 0)) if _vegeta_reg.get("at") else None,
            "enabled": bool(MEDIAFLOW_URL),
        },
        "unified": len(_unified_registry()),
        "lang_counts": lang_counts,
        "history": [list(pair) for pair in _hist],
        "hist_err": [list(pair) for pair in _hist_err],
        "health": _health_snapshot,
        "daily_totals": _daily_totals(),
        "top_channels": _top_channels(),
        "recent_plays": _recent_plays_list(),
    }

def _public_stats() -> dict:
    all_ch = channels()
    vavoo_ch = vavoo_channels()
    return {
        "version": _VERSION,
        "uptime": int(time.time() - _START_TIME),
        "channels_total": len(all_ch) + len(vavoo_ch),
        "dlstreams_count": len(_ch_cache.get("list") or []),
        "vavoo_count": len(vavoo_ch),
        "manual_count": len(_manual_channels),
    }

def _build_m3u(qs: dict) -> str:
    base = qs.get("base", [""])[0] or ""
    code = qs.get("code", [""])[0]
    playlist_name = qs.get("playlist", [""])[0]
    wp = _settings.get("wiseplay", {})
    stored_code = wp.get("access_code", "")
    wiseplay_mode = bool(code) and bool(stored_code) and code == stored_code

    playlist_filter = None
    if wiseplay_mode and playlist_name:
        pl = wp.get("playlists", {}).get(playlist_name)
        pl_channels = pl.get("channels", []) if isinstance(pl, dict) else (pl or [])
        playlist_filter = {str(cid) for cid in pl_channels}

    if wiseplay_mode:
        ch_toggles = wp.get("channels", {})
        src = wp.get("sources", {"dlstreams": True, "vavoo": True, "vegetatv": True})
        include_dlstreams = src.get("dlstreams", True)
        include_vavoo = src.get("vavoo", True)
        include_vegetatv = src.get("vegetatv", True)
    else:
        ch_toggles = {}
        include_dlstreams = True
        include_vavoo = qs.get("vavoo", ["true"])[0] == "true"
        include_vegetatv = qs.get("vegetatv", ["true"])[0] == "true"

    def _keep(cid) -> bool:
        if playlist_filter is not None:
            return str(cid) in playlist_filter
        return ch_toggles.get(str(cid)) is not False

    # group-title = GENRE (même taxo que Stremio : « sport dans sport »), plus la langue.
    # tvg-logo pointe sur la route /logo de l'addon -> profite des logos curés (dossier LOGOS/).
    lines = ["#EXTM3U"]
    if include_dlstreams:
        for ch in channels():
            if not _keep(ch["id"]):
                continue
            enc = urllib.parse.quote(str(ch["id"]), safe="")
            grp = _genres_for(ch["name"])[0]
            lines.append(f'#EXTINF:-1 tvg-id="{ch["id"]}" tvg-logo="{base}/logo/dlstreams/{enc}.png" group-title="{grp}",{ch["name"]}')
            lines.append(f"{base}/hls/{ch['id']}/index.m3u8")
    if include_vavoo:
        for ch in vavoo_channels():
            if not _keep(ch["id"]):
                continue
            enc = _b64u(ch["id"])
            grp = _genres_for(ch["name"])[0]
            lines.append(f'#EXTINF:-1 tvg-id="vavoo:{ch["id"]}" tvg-logo="{base}/logo/vavoo/{urllib.parse.quote(enc, safe="")}.png" group-title="{grp}",{ch["name"]}')
            lines.append(f"{base}/vhls?v={enc}")
    if include_vegetatv and MEDIAFLOW_URL:
        for ch in vegetatv_channels():
            if not _keep(ch["id"]):
                continue
            enc = _b64u(ch["id"])
            grp = _genres_for(ch["name"])[0]
            lines.append(f'#EXTINF:-1 tvg-id="vegetatv:{ch["id"]}" tvg-logo="{base}/logo/vegetatv/{urllib.parse.quote(enc, safe="")}.png" group-title="{grp}",{ch["name"]}')
            lines.append(f"{base}/vghls?v={enc}")
    if playlist_filter is None:
        st = _settings.get("stremio", {})
        for cid, cc in st.get("custom_channels", {}).items():
            grp = _genres_for(cc.get("name", ""))[0]
            logo = cc.get("logo", "")
            for idx, stream_url in enumerate(cc.get("streams", [])):
                lines.append(f'#EXTINF:-1 tvg-id="custom:{cid}" tvg-logo="{logo}" group-title="{grp}",{cc.get("name", cid)}')
                lines.append(f"{base}/hls/custom/{cid}/s{idx}/index.m3u8")
    return "\n".join(lines)

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
                if bits & (1 << col):
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
    c1, c2 = (15, 23, 42), (30, 27, 75)
    accent = (99, 102, 241)
    pink = (236, 72, 153)
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
    while any(_text_width(l, scale) > max_w for l in lines) and scale > 2:
        scale -= 1
    total_h = len(lines) * 7 * scale + (len(lines) - 1) * scale
    ty = (H - total_h) // 2 - 2
    for line in lines:
        tw = _text_width(line, scale)
        _draw_text(buf, W, line, (W - tw) // 2, ty, scale, (241, 245, 249))
        ty += 8 * scale
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

_logo_cache: dict[str, bytes] = {}
_logo_bad: set[str] = set()

_health_snapshot: dict = {"at": 0.0}
_health_lock = threading.Lock()
_HEALTH_TTL = 60

def _health_refresh(force: bool = False):
    with _health_lock:
        if not force and time.time() - _health_snapshot.get("at", 0) < _HEALTH_TTL:
            return
    def _dl():
        t0 = time.time()
        try:
            _get(SITE + "/", timeout=8)
            return {"ok": True, "ms": int((time.time() - t0) * 1000)}
        except Exception as e:
            return {"ok": False, "ms": int((time.time() - t0) * 1000)}
    def _vv():
        t0 = time.time()
        try:
            _post_json(_VAVOO_PINGS[0], _vavoo_ping_body(), {"user-agent": _VAVOO_UA}, timeout=8)
            return {"ok": True, "ms": int((time.time() - t0) * 1000)}
        except Exception as e:
            return {"ok": False, "ms": int((time.time() - t0) * 1000)}
    with ThreadPoolExecutor(max_workers=2) as ex:
        dl = ex.submit(_dl).result()
        vv = ex.submit(_vv).result()
    with _epg_lock:
        epg_channels = len(_epg_data)
        epg_age = int(time.time() - _epg_at) if _epg_at else None
        epg_ok = epg_channels > 0 and (epg_age is not None and epg_age < 36 * 3600)
    logos_total = len(_CH_LOGO)
    logos_loaded = len(_logo_cache)
    logos_ok = logos_total > 0 and logos_loaded >= max(1, logos_total // 2)
    # VegetaTv : sain = MFP configuré ET registre peuplé ET au moins un serveur up.
    vg_n = len(_vegeta_reg.get("reg") or {})
    vg = {"ok": bool(MEDIAFLOW_URL) and vg_n > 0, "channels": vg_n, "enabled": bool(MEDIAFLOW_URL)}
    with _health_lock:
        _health_snapshot.update(
            at=time.time(),
            dlstreams=dl, vavoo=vv, vegetatv=vg,
            epg={"ok": epg_ok, "channels": epg_channels, "age": epg_age},
            logos={"ok": logos_ok, "loaded": logos_loaded, "total": logos_total},
        )

def _now_playing() -> list[dict]:
    # NE PAS tenir _epg_lock ici : _epg_slot() le reprend lui-même et threading.Lock n'est PAS
    # ré-entrant -> auto-deadlock qui empoisonnait le lock (cassait aussi /api/settings et /api/health).
    if not _epg_data:
        return []
    out = []
    for c in _POPULAR_CHANNELS:
        cur, nxt = _epg_slot(c["id"])       # acquiert _epg_lock brièvement, par chaîne
        if not cur:
            continue
        out.append({
            "id": c["id"],
            "name": c["name"],
            "logo": f"/logo/dlstreams/{c['id']}.png",
            "now": int(time.time()),
            "cur": {"title": cur.get("title", ""), "desc": cur.get("desc", ""), "start": cur.get("start", 0), "stop": cur.get("stop", 0)},
            "nxt": {"title": (nxt or {}).get("title", ""), "start": (nxt or {}).get("start", 0)} if nxt else None,
        })
    return out

_TVLOGOS_BASE = "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/france/"
_remote_logo_cache: dict = {}   # slug -> bytes | None (négatif caché)

def _tvlogos_slug(name: str) -> str:
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    s = s.replace("+", " plus ")
    s = re.sub(r"\b(hd|fhd|uhd|4k|8k|sd|fr|vip|raw|backup|event|only)\b", " ", s)
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")

def _remote_logo(name: str):
    """Fallback logo distant via tv-logo/tv-logos (France). 2 variantes de nom, résultat caché."""
    slug = _tvlogos_slug(name)
    if not slug:
        return None
    if slug in _remote_logo_cache:
        return _remote_logo_cache[slug]
    png = None
    for suffix in ("-fr.png", "-french-fr.png"):
        try:
            req = urllib.request.Request(_TVLOGOS_BASE + slug + suffix, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=8) as r:
                b = r.read()
            if b[:3] in (b"\xff\xd8\xff", b"\x89PN"):
                png = b
                break
        except Exception:
            continue
    if len(_remote_logo_cache) > 3000:
        _remote_logo_cache.clear()
    _remote_logo_cache[slug] = png
    return png

def _logo_bytes(src: str, c: dict) -> bytes:
    if not _settings.get("logos", True):
        return _poster_get(c.get("name") or "TV")
    name = c.get("name") or ""
    cid = str(c.get("id") or "")
    st = _settings.get("stremio", {})
    manual = st.get("channel_logos", {}).get(cid)
    if manual:
        url = manual.strip()
    else:
        # Logo local curé (le dossier LOGOS/) prime sur les logos distants.
        loc = _local_logo_for(name)
        if loc:
            b = _read_local_logo(loc[0])
            if b:
                return b
        if src == "dlstreams":
            url = _CH_LOGO.get(cid, "")
        else:
            url = (c.get("logo") or "").strip()
    if not url and name:
        url = _LOGO_BY_NAME.get(_norm_name(name), "")
    if url and name:
        key = _norm_name(name)
        if key and key not in _LOGO_BY_NAME:
            _LOGO_BY_NAME[key] = url
    if url:
        png = _logo_cache.get(url)
        if png is None and url not in _logo_bad:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=10) as r:
                    png = r.read()
                if png[:3] not in (b"\xff\xd8\xff", b"\x89PN"):
                    raise ValueError("pas une image")
            except Exception:
                png = None
                _logo_bad.add(url)
            if png is not None:
                if len(_logo_cache) > 500:
                    _logo_cache.clear()
                _logo_cache[url] = png
        if png:
            return png
    return _poster_get(name or "TV")

def _warm_logos():
    for url in _CH_LOGO.values():
        try:
            if url not in _logo_cache and url not in _logo_bad:
                req = urllib.request.Request(url, headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=15) as r:
                    b = r.read()
                if b[:3] in (b"\xff\xd8\xff", b"\x89PN") and len(_logo_cache) < 400:
                    _logo_cache[url] = b
        except Exception as e:
            _logo_bad.add(url)

# ============================================================
# HTML FILES - Chargés depuis le disque au lieu d'être hardcodés
# ============================================================
_HTML_DIR = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_HTML = ""
CONFIGURE_HTML = ""
WISEPLAY_HTML = ""

def _load_html_files():
    global DASHBOARD_HTML, CONFIGURE_HTML, WISEPLAY_HTML
    try:
        with open(os.path.join(_HTML_DIR, "dashboard.html"), "r", encoding="utf-8") as f:
            DASHBOARD_HTML = f.read()
        log.info(f"dashboard.html chargé ({len(DASHBOARD_HTML)} octets)")
    except Exception as e:
        log.error(f"Impossible de lire dashboard.html: {e}")
        DASHBOARD_HTML = "<html><body><h1>dashboard.html introuvable</h1></body></html>"
    
    try:
        with open(os.path.join(_HTML_DIR, "configure.html"), "r", encoding="utf-8") as f:
            CONFIGURE_HTML = f.read()
        log.info(f"configure.html chargé ({len(CONFIGURE_HTML)} octets)")
    except Exception as e:
        log.error(f"Impossible de lire configure.html: {e}")
        CONFIGURE_HTML = "<html><body><h1>configure.html introuvable</h1></body></html>"
    
    try:
        with open(os.path.join(_HTML_DIR, "wiseplay.html"), "r", encoding="utf-8") as f:
            WISEPLAY_HTML = f.read()
        log.info(f"wiseplay.html chargé ({len(WISEPLAY_HTML)} octets)")
    except Exception as e:
        log.error(f"Impossible de lire wiseplay.html: {e}")
        WISEPLAY_HTML = "<html><body><h1>wiseplay.html introuvable</h1></body></html>"

_load_html_files()
# ============================================================

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
                if _hist_err and _hist_err[-1][0] == now_min:
                    _hist_err[-1][1] += 1
                else:
                    _hist_err.append([now_min, 1])
                while len(_hist_err) > 1 and _hist_err[0][0] < now_min - _HIST_KEEP_MIN:
                    _hist_err.pop(0)
            else:
                if _hist and _hist[-1][0] == now_min:
                    _hist[-1][1] += 1
                else:
                    _hist.append([now_min, 1])
                while len(_hist) > 1 and _hist[0][0] < now_min - _HIST_KEEP_MIN:
                    _hist.pop(0)
            _hist_save()
        if not self.path.startswith(("/api/stats", "/api/logs", "/api/activity",
            "/api/live", "/api/now", "/api/health")):
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
    def _user_config(self) -> dict:
        cfg_b64 = self._cookie("waddontv_cfg")
        if not cfg_b64:
            return {}
        try:
            return json.loads(_unb64u(cfg_b64))
        except Exception:
            return {}
    def _extract_addon_config(self, path: str) -> tuple[dict, str]:
        user_config, clean_path = _extract_config_from_path(path)
        if not user_config:
            user_config = self._user_config()
        return user_config, clean_path
    def _authed(self) -> bool:
        tok = self._cookie("dl_session")
        if not tok:
            return False
        with _stats_lock:
            issued = _sessions.get(tok)
            return bool(issued) and (time.time() - issued) < _SESSION_TTL
    def _require_auth(self) -> bool:
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
        qs = urllib.parse.parse_qs(u.query)
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
            except Exception as e:
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
        if path == "/api/identity":
            try:
                data = json.loads(body) if body else {}
            except Exception as e:
                data = {}
            pseudo = str(data.get("pseudo", "")).strip()[:32]
            device = str(data.get("device", "")).strip()[:32]
            ip = self._client_ip()
            if pseudo:
                _log_activity("Identité déclarée", f"{pseudo} ({device or 'appareil inconnu'}) — IP: {ip}")
            resp = json.dumps({"success": True}).encode()
            self._send(200, resp, "application/json")
            return
        if path == "/api/unified/edit":
            # Édition d'UNE chaîne unifiée : nom / logo / flux perso (merge partiel par canon_key).
            if not self._require_auth():
                return
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {}
            key = str(data.get("key") or "")
            if not key:
                return self._send(400, json.dumps({"ok": False, "error": "clé manquante"}).encode(), "application/json")
            st = _settings.setdefault("stremio", {})
            for field, val in (("channel_names", data.get("name")), ("channel_logos", data.get("logo"))):
                d = st.setdefault(field, {})
                if val:
                    d[key] = str(val).strip()
                else:
                    d.pop(key, None)          # vide -> retire l'override (retour à l'auto)
            streams = data.get("streams")
            if isinstance(streams, list):
                clean = [{"label": str(s.get("label", "")).strip(), "url": str(s.get("url", "")).strip()}
                         for s in streams if isinstance(s, dict) and s.get("url")]
                cs = st.setdefault("channel_streams", {})
                if clean:
                    cs[key] = clean
                else:
                    cs.pop(key, None)
            _settings_save()
            _unified_invalidate()
            return self._send(200, json.dumps({"ok": True}).encode(), "application/json")
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
            except Exception as e:
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
            except Exception as e:
                pass
            threading.Timer(0.5, lambda: os._exit(0)).start()
            return self._send(200, json.dumps({"success": True, "message": "Redémarrage en cours..."}).encode(), "application/json")
        if path == "/api/settings":
            if not self._require_auth():
                return
            try:
                data = json.loads(body) if body else {}
            except Exception as e:
                data = {}
            changed = False
            for k in ("logos", "epg"):
                if k in data and isinstance(data[k], bool) and _settings.get(k) != data[k]:
                    _settings[k] = data[k]
                    changed = True
            if isinstance(data.get("epg_url"), str) and data["epg_url"].strip():
                _settings["epg_url"] = data["epg_url"].strip()
                changed = True
            if isinstance(data.get("genres"), dict):
                _settings["genres"] = {str(k).lower(): [str(g) for g in v]
                    for k, v in data["genres"].items()}
                changed = True
            if isinstance(data.get("stremio"), dict):
                st = data["stremio"]
                for k in ("manifest_name", "manifest_desc", "default_lang"):
                    if isinstance(st.get(k), str):
                        _settings["stremio"][k] = st[k]
                        changed = True
                for k in ("include_dlstreams", "include_vavoo"):
                    if isinstance(st.get(k), bool):
                        _settings["stremio"][k] = st[k]
                        changed = True
                for k in ("channel_names", "channel_logos", "channel_epg"):
                    if isinstance(st.get(k), dict):
                        _settings["stremio"][k] = {str(kk): str(vv) for kk, vv in st[k].items()}
                        changed = True
                if isinstance(st.get("channel_streams"), dict):
                    _settings["stremio"]["channel_streams"] = {}
                    for kk, vv in st["channel_streams"].items():
                        items = vv if isinstance(vv, list) else [vv]
                        streams = []
                        for v in items:
                            if isinstance(v, dict) and v.get("url"):
                                streams.append({"url": str(v["url"]), "label": str(v.get("label", ""))})
                            elif isinstance(v, str) and v.strip():
                                streams.append({"url": v.strip(), "label": ""})
                        if streams:
                            _settings["stremio"]["channel_streams"][str(kk)] = streams
                    changed = True
                if isinstance(st.get("custom_channels"), dict):
                    _settings["stremio"]["custom_channels"] = {}
                    for kk, vv in st["custom_channels"].items():
                        if isinstance(vv, dict):
                            _settings["stremio"]["custom_channels"][str(kk)] = {
                                "name": str(vv.get("name", "")),
                                "logo": str(vv.get("logo", "")),
                                "streams": [str(s) for s in (vv.get("streams") or []) if isinstance(s, str)],
                                "epg": str(vv.get("epg", "")),
                            }
                    changed = True
            if isinstance(data.get("wiseplay"), dict):
                wp = data["wiseplay"]
                if isinstance(wp.get("access_code"), str):
                    _settings["wiseplay"]["access_code"] = wp["access_code"]
                    changed = True
                if isinstance(wp.get("channels"), dict):
                    _settings["wiseplay"]["channels"] = {str(k): bool(v) for k, v in wp["channels"].items()}
                    changed = True
                if isinstance(wp.get("playlists"), dict):
                    _settings["wiseplay"]["playlists"] = {str(k): list(v) for k, v in wp["playlists"].items()}
                    changed = True
                if isinstance(wp.get("sources"), dict):
                    for k in ("dlstreams", "vavoo", "vegetatv"):
                        if isinstance(wp["sources"].get(k), bool):
                            _settings["wiseplay"]["sources"][k] = wp["sources"][k]
                    changed = True
            if changed:
                _settings_save()
            return self._send(200, json.dumps({"success": True, "settings": _settings}).encode(),
                "application/json")
        if path == "/api/epg/refresh":
            if not self._require_auth():
                return
            threading.Thread(target=_epg_refresh, args=(True,), daemon=True).start()
            return self._send(200, json.dumps({"success": True,
                "message": "rafraichissement EPG lance"}).encode(),
                "application/json")
        if path == "/api/playlists":
            if not self._require_auth():
                return
            try:
                data = json.loads(body) if body else {}
            except Exception as e:
                return self._send(400, json.dumps({"success": False, "error": "body invalide"}).encode(), "application/json")
            action = data.get("action")
            if action == "create":
                name = data.get("name", "").strip()
                if not name:
                    return self._send(400, json.dumps({"success": False, "error": "nom requis"}).encode(), "application/json")
                if any(p["name"] == name for p in _playlists):
                    return self._send(400, json.dumps({"success": False, "error": "nom déjà utilisé"}).encode(), "application/json")
                _playlists.append({"name": name, "channels": []})
                _playlists_save()
                return self._send(200, json.dumps({"success": True, "playlists": _playlists}).encode(), "application/json")
            if action == "add":
                name = data.get("name", "").strip()
                key = data.get("key", "").strip()
                if not name or not key:
                    return self._send(400, json.dumps({"success": False, "error": "nom et key requis"}).encode(), "application/json")
                pl = next((p for p in _playlists if p["name"] == name), None)
                if not pl:
                    return self._send(404, json.dumps({"success": False, "error": "playlist introuvable"}).encode(), "application/json")
                if any(c["key"] == key for c in pl.get("channels", [])):
                    return self._send(400, json.dumps({"success": False, "error": "chaîne déjà dans la playlist"}).encode(), "application/json")
                pl.setdefault("channels", []).append({"key": key})
                _playlists_save()
                return self._send(200, json.dumps({"success": True, "playlists": _playlists}).encode(), "application/json")
            if action == "remove":
                name = data.get("name", "").strip()
                key = data.get("key", "").strip()
                if not name or not key:
                    return self._send(400, json.dumps({"success": False, "error": "nom et key requis"}).encode(), "application/json")
                pl = next((p for p in _playlists if p["name"] == name), None)
                if not pl:
                    return self._send(404, json.dumps({"success": False, "error": "playlist introuvable"}).encode(), "application/json")
                pl["channels"] = [c for c in pl.get("channels", []) if c["key"] != key]
                _playlists_save()
                return self._send(200, json.dumps({"success": True, "playlists": _playlists}).encode(), "application/json")
            if action == "delete":
                name = data.get("name", "").strip()
                if not name:
                    return self._send(400, json.dumps({"success": False, "error": "nom requis"}).encode(), "application/json")
                idx = next((i for i, p in enumerate(_playlists) if p["name"] == name), None)
                if idx is None:
                    return self._send(404, json.dumps({"success": False, "error": "playlist introuvable"}).encode(), "application/json")
                _playlists.pop(idx)
                _playlists_save()
                return self._send(200, json.dumps({"success": True, "playlists": _playlists}).encode(), "application/json")
            return self._send(400, json.dumps({"success": False, "error": "action inconnue"}).encode(), "application/json")
        if path == "/api/wiseplay/channel-edit":
            # Édition d'une chaîne unifiée depuis Wiseplay (auth par code d'accès). Merge partiel.
            code = qs.get("code", [""])[0]
            stored_code = _settings.get("wiseplay", {}).get("access_code", "")
            if not stored_code or code != stored_code:
                return self._send(401, json.dumps({"ok": False, "error": "Code invalide"}).encode(), "application/json")
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {}
            key = str(data.get("key") or "")
            if not key:
                return self._send(400, json.dumps({"ok": False, "error": "clé manquante"}).encode(), "application/json")
            st = _settings.setdefault("stremio", {})
            for field, val in (("channel_names", data.get("name")), ("channel_logos", data.get("logo"))):
                d = st.setdefault(field, {})
                if val:
                    d[key] = str(val).strip()
                else:
                    d.pop(key, None)
            streams = data.get("streams")
            if isinstance(streams, list):
                clean = [{"label": str(s.get("label", "")).strip(), "url": str(s.get("url", "")).strip()}
                         for s in streams if isinstance(s, dict) and s.get("url")]
                cs = st.setdefault("channel_streams", {})
                if clean:
                    cs[key] = clean
                else:
                    cs.pop(key, None)
            _settings_save()
            _unified_invalidate()
            return self._send(200, json.dumps({"ok": True}).encode(), "application/json")
        if path == "/api/wiseplay/config":
            code = qs.get("code", [""])[0]
            stored_code = _settings.get("wiseplay", {}).get("access_code", "")
            if not stored_code or code != stored_code:
                return self._send(401, json.dumps({"success": False, "error": "Code invalide ou non configuré"}).encode(), "application/json")
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {}
            changed = False
            wp = _settings.setdefault("wiseplay", {})
            if "channels" in data and isinstance(data["channels"], dict):
                wp["channels"] = {str(k): bool(v) for k, v in data["channels"].items()}
                changed = True
            if "playlists" in data and isinstance(data["playlists"], dict):
                payload = data["playlists"]
                wp.setdefault("playlists", {})
                if "_delete" in payload:
                    wp["playlists"].pop(str(payload["_delete"]), None)
                for k, v in payload.items():
                    if k == "_delete":
                        continue
                    k = str(k)
                    existing = wp["playlists"].get(k)
                    existing_logo = existing.get("logo", "") if isinstance(existing, dict) else ""
                    if isinstance(v, dict):
                        pl_channels = [str(x) for x in v.get("channels", [])]
                        pl_logo = str(v["logo"]) if "logo" in v else existing_logo
                        wp["playlists"][k] = {"channels": pl_channels, "logo": pl_logo}
                    elif isinstance(v, list):
                        wp["playlists"][k] = {"channels": [str(x) for x in v], "logo": existing_logo}
                changed = True
            if "sources" in data and isinstance(data["sources"], dict):
                for k in ("dlstreams", "vavoo", "vegetatv"):
                    if k in data["sources"] and isinstance(data["sources"][k], bool):
                        wp.setdefault("sources", {})[k] = data["sources"][k]
                        changed = True
            if changed:
                _settings_save()
            return self._send(200, json.dumps(wp).encode(), "application/json")

        # ========== TOKENS ADDON ==========
        if path == "/api/tokens":
            # Admin: gestion tokens (créer, lister, révoquer) — protégé par dashboard password
            if not self._require_auth():
                return
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {}
            action = data.get("action")
            if action == "create":
                name = str(data.get("name", "")).strip()
                if not name:
                    return self._send(400, json.dumps({"ok": False, "error": "nom requis"}).encode(), "application/json")
                token = _token_gen()
                h = _token_hash(token)
                _tokens[h] = {
                    "name": name,
                    "created": time.time(),
                    "last_used": None,
                    "revoked": False
                }
                _tokens_save()
                _log_activity("Token créé", f"name={name}")
                return self._send(200, json.dumps({"ok": True, "token": token, "name": name}).encode(), "application/json")
            if action == "list":
                out = []
                for h, info in _tokens.items():
                    out.append({
                        "hash": h[:12] + "...",
                        "full_hash": h,
                        "name": info.get("name", ""),
                        "created": info.get("created"),
                        "last_used": info.get("last_used"),
                        "revoked": info.get("revoked", False)
                    })
                out.sort(key=lambda x: x.get("created") or 0, reverse=True)
                return self._send(200, json.dumps({"ok": True, "tokens": out}).encode(), "application/json")
            if action == "revoke":
                h = str(data.get("hash", "")).strip()
                if not h:
                    return self._send(400, json.dumps({"ok": False, "error": "hash requis"}).encode(), "application/json")
                # Accepte hash complet ou préfixe (12 premiers chars)
                full_hash = None
                for stored_h in _tokens:
                    if stored_h == h or stored_h.startswith(h.replace("...", "")):
                        full_hash = stored_h
                        break
                if not full_hash:
                    return self._send(404, json.dumps({"ok": False, "error": "token introuvable"}).encode(), "application/json")
                name = _tokens[full_hash].get("name", "")
                del _tokens[full_hash]
                _tokens_save()
                _log_activity("Token supprimé", f"name={name}")
                return self._send(200, json.dumps({"ok": True}).encode(), "application/json")
            return self._send(400, json.dumps({"ok": False, "error": "action inconnue"}).encode(), "application/json")

        if path == "/api/validate-token":
            # User (page Configure): validation token + renvoi lien manifest
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {}
            token = str(data.get("token", "")).strip()
            if not token:
                return self._send(400, json.dumps({"ok": False, "error": "token requis"}).encode(), "application/json")
            valid, h = _token_verify(token)
            if not valid:
                return self._send(401, json.dumps({"ok": False, "error": "token invalide ou révoqué"}).encode(), "application/json")
            _token_touch(h)
            base = self._self_base()
            manifest_url = f"{base}/manifest.json?token={token}"
            return self._send(200, json.dumps({"ok": True, "manifest_url": manifest_url, "name": _tokens[h].get("name", "")}).encode(), "application/json")

        return self._send(404, b"not found", "text/plain")
    def do_GET(self):
        u = urllib.parse.urlsplit(self.path)
        path = u.path
        qs = urllib.parse.parse_qs(u.query)
        try:
            if path.startswith("/logo/") and path.endswith(".png"):
                seg = urllib.parse.unquote(path[len("/logo/"):-4])
                src, _, cid = seg.partition("/")
                if src == "vavoo":
                    url = _unb64u(cid)
                    c = next(
                        (x for x in vavoo_channels()
                        if str(x.get("id", "")).strip() == str(url).strip()),
                        None
                    )
                    if c is None:
                        c = {"id": url, "name": "Vavoo", "logo": ""}
                elif src == "vegetatv":
                    key = _unb64u(cid)
                    c = next((x for x in vegetatv_channels()
                        if str(x.get("id", "")).strip() == str(key).strip()),
                        {"id": key, "name": "Vegeta TV", "logo": ""})
                elif src == "unified":
                    key = _unb64u(cid)
                    e = _unified_registry().get(key)
                    c = {"id": key, "name": e["name"] if e else "TV", "logo": ""}
                else:
                    c = next((x for x in channels() if str(x.get("id")) == str(cid)),
                        {"id": cid, "name": f"dlstreams {cid}", "logo": ""})
                return self._send(200, _logo_bytes(src, c), "image/png", True)
            if path.startswith("/poster/") and path.endswith(".png"):
                pname = urllib.parse.unquote(path[len("/poster/"):-4])
                return self._send(200, _poster_get(pname), "image/png", True)
            if path == "/dashboard" or path == "/dashboard.html" or path.startswith("/dashboard/"):
                return self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8", True)
            if path == "/wiseplay" or path == "/wiseplay.html":
                return self._send(200, WISEPLAY_HTML.encode("utf-8"), "text/html; charset=utf-8", True)
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
            if path == "/api/public-stats":
                return self._send(200, json.dumps(_public_stats()).encode(), "application/json", True)
            if path == "/playlist.m3u":
                return self._send(200, _build_m3u(qs).encode("utf-8"), "application/vnd.apple.mpegurl", True)
            if path == "/api/stats":
                if not self._require_auth():
                    return
                threading.Thread(target=_health_refresh, daemon=True).start()
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
            if path == "/api/live":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(_live_plays()).encode(), "application/json")
            if path == "/api/now":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(_now_playing()).encode(), "application/json")
            if path == "/api/settings":
                if not self._require_auth():
                    return
                with _epg_lock:
                    epg_covered = len(_epg_data)
                    epg_at = _epg_at
                return self._send(200, json.dumps({
                    "settings": _settings,
                    "epg": {"at": epg_at, "covered": epg_covered, "channels": len(_CH_EPG)},
                    "genre_choices": _GENRE_CHOICES,
                    "popular": [{"id": c["id"], "name": c["name"], "genre": _genres_for(c["name"])[0]}
                        for c in _POPULAR_CHANNELS],
                    "epg_map": _CH_EPG,
                    "version": _VERSION}).encode(), "application/json")
            if path == "/api/vavoo-channels":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(vavoo_channels()).encode(), "application/json", True)
            if path == "/api/manual-channels":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(list(_manual_channels.values())).encode(), "application/json")
            if path == "/api/unified":
                # Registre unifié (toutes sources fusionnées) pour le dashboard + le check des logos.
                if not self._require_auth():
                    return
                base = self._self_base()
                st = _settings.get("stremio", {})
                ov_n, ov_l, ov_s = st.get("channel_names", {}), st.get("channel_logos", {}), st.get("channel_streams", {})
                out = []
                for e in sorted(_unified_registry().values(), key=lambda e: (e["cat"], e["name"].lower())):
                    enc = urllib.parse.quote(_b64u(e["key"]), safe="")
                    out.append({
                        "key": e["key"], "name": e["name"], "cat": e["cat"],
                        "logo": f"{base}/logo/unified/{enc}.png",
                        "sources": sorted({r["src"] for r in e["refs"]}),
                        "flux": len(e["refs"]),
                        "quals": list(dict.fromkeys(r["q"] for r in e["refs"] if r["q"])),
                        "curated": _local_logo_for(e["name"]) is not None,
                        "override_name": ov_n.get(e["key"], ""),
                        "override_logo": ov_l.get(e["key"], ""),
                        "override_streams": ov_s.get(e["key"], []),
                    })
                return self._send(200, json.dumps({"channels": out, "total": len(out)}).encode(),
                    "application/json")
            if path == "/api/vegeta/refresh":
                # Force une ingestion VegetaTv SYNCHRONE + renvoie le diagnostic (débogage MFP/réseau).
                if not self._require_auth():
                    return
                if not MEDIAFLOW_URL:
                    return self._send(200, json.dumps({"ok": False,
                        "error": "MEDIAFLOW_URL non configuré (VegetaTv désactivé)"}).encode(), "application/json")
                try:
                    _unified_invalidate()
                    n = _vegeta_ingest()
                except Exception as e:
                    return self._send(200, json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}).encode(),
                        "application/json")
                return self._send(200, json.dumps({"ok": n > 0, "channels": n, "diag": _vegeta_diag}).encode(),
                    "application/json")
            if path == "/api/playlists":
                if not self._require_auth():
                    return
                return self._send(200, json.dumps(_playlists).encode(), "application/json")
            if path == "/api/wiseplay/config":
                code = qs.get("code", [""])[0]
                stored_code = _settings.get("wiseplay", {}).get("access_code", "")
                if not stored_code or code != stored_code:
                    return self._send(401, json.dumps({"success": False, "error": "Code invalide ou non configuré"}).encode(), "application/json")
                # Wiseplay UNIFIÉ : chaînes fusionnées (façon addon), pour une navigation par CATÉGORIE.
                base = self._self_base()
                st = _settings.get("stremio", {})
                ov_n, ov_l, ov_s = st.get("channel_names", {}), st.get("channel_logos", {}), st.get("channel_streams", {})
                unified = []
                for e in sorted(_unified_registry().values(), key=lambda e: (e["cat"], e["name"].lower())):
                    enc = urllib.parse.quote(_b64u(e["key"]), safe="")
                    unified.append({"key": e["key"], "name": e["name"], "cat": e["cat"],
                        "logo": f"{base}/logo/unified/{enc}.png",
                        "sources": sorted({r["src"] for r in e["refs"]}), "flux": len(e["refs"]),
                        "override_name": ov_n.get(e["key"], ""), "override_logo": ov_l.get(e["key"], ""),
                        "override_streams": ov_s.get(e["key"], [])})
                wp = _settings.get("wiseplay", {})
                return self._send(200, json.dumps({**wp, "unified": unified,
                    "categories": unified_categories()}).encode(), "application/json")
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
            if path == "/api/public-check":
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
                    _log_activity("Test flux public", f"#{cid} OK en {ms}ms")
                    return self._send(200, json.dumps({"ok": True, "ms": ms, "url": url}).encode(), "application/json")
                except Exception as e:
                    ms = int((time.time() - t0) * 1000)
                    _log_activity("Test flux public", f"#{cid} échec ({type(e).__name__})")
                    return self._send(200, json.dumps({"ok": False, "ms": ms, "error": str(e)}).encode(), "application/json")
            if path == "/api/health":
                # Non-bloquant : renvoie le snapshot en cache ; rafraîchit en FOND si périmé
                # (les sondes réseau 8s+8s ne doivent JAMAIS bloquer la requête du dashboard).
                if time.time() - _health_snapshot.get("at", 0) >= _HEALTH_TTL:
                    threading.Thread(target=_health_refresh, args=(True,), daemon=True).start()
                return self._send(200, json.dumps({
                    "dlstreams": {"ok": _health_snapshot.get("dlstreams", {}).get("ok", False)},
                    "vavoo": {"ok": _health_snapshot.get("vavoo", {}).get("ok", False)},
                    "epg": {"ok": _health_snapshot.get("epg", {}).get("ok", False)},
                }).encode(), "application/json")
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

            def _check_manifest_token(qs: dict) -> tuple[bool, str | None]:
                """Vérifie le token dans query string. Retourne (valide, token_hash_or_None)."""
                token = qs.get("token", [""])[0]
                if not token:
                    return False, None
                return _token_verify(token)

            if path.startswith("/") and path.endswith("/manifest.json"):
                # Vérifier token AVANT de décoder config_b64
                valid, token_hash = _check_manifest_token(qs)
                if not valid:
                    self._send(401, json.dumps({"error": "token requis ou invalide", "code": "TOKEN_REQUIRED"}).encode(), "application/json")
                    return
                config_b64 = path[1:-len("/manifest.json")]
                user_config = _decode_config(config_b64) if config_b64 else {}
                lang_filter = user_config.get("lang", "fr")
                body = json.dumps(self._manifest(lang_filter=lang_filter, user_config=user_config)).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "public, max-age=60")
                if user_config:
                    cookie_val = _b64u(json.dumps(user_config))
                    self.send_header("Set-Cookie", f"waddontv_cfg={cookie_val}; HttpOnly; Path=/; Max-Age=31536000; SameSite=Lax")
                # Toucher le token pour last_used
                if token_hash:
                    _token_touch(token_hash)
                self.end_headers()
                self.wfile.write(body)
                return
            if path in ("/", "/manifest.json"):
                valid, token_hash = _check_manifest_token(qs)
                if not valid:
                    return self._send(401, json.dumps({"error": "token requis ou invalide", "code": "TOKEN_REQUIRED"}).encode(), "application/json")
                if token_hash:
                    _token_touch(token_hash)
                return self._send(200, json.dumps(self._manifest(lang_filter="fr")).encode(), "application/json", True)
            user_config, clean_path = self._extract_addon_config(path)
            if clean_path.startswith("/catalog/tv/"):
                extra = clean_path[len("/catalog/tv/"):].removesuffix(".json")
                catid = extra.split("/", 1)[0]
                params = {}
                if "/" in extra:
                    rest = extra.split("/", 1)[1]
                    if rest.startswith("genre/"):
                        params["genre"] = urllib.parse.unquote_plus(rest[len("genre/"):])
                    else:
                        for kv in rest.split("&"):
                            if "=" in kv:
                                k, v = kv.split("=", 1)
                                params[k] = urllib.parse.unquote_plus(v)
                lang_filter = user_config.get("lang", "fr")
                if catid == "custom":
                    st = _settings.get("stremio", {})
                    custom_channels = st.get("custom_channels", {})
                    metas = []
                    for cid, cc in custom_channels.items():
                        c = {"id": cid, "name": cc.get("name", "Custom"), "logo": cc.get("logo", ""), "lang": lang_filter}
                        metas.append(self._meta(c, "custom", user_config))
                    skip = int(params.get("skip") or 0)
                    metas = metas[skip:skip + 100]
                    return self._send(200, json.dumps({"metas": metas}).encode(), "application/json", True)
                if catid == "waddontv" or catid.startswith("u_"):
                    g = params.get("genre") or (_SLUG_CAT.get(catid[2:], catid[2:]) if catid.startswith("u_") else "")
                    if g:                          # une catégorie précise
                        entries = _unified_by_cat(g)
                    else:                          # tout le catalogue, rangé par catégorie
                        entries = sorted(_unified_registry().values(),
                            key=lambda e: (_GENRE_CHOICES.index(e["cat"]) if e["cat"] in _GENRE_CHOICES else 99, e["name"].lower()))
                    q = params.get("search", "").lower().strip()
                    if q:
                        words = q.replace("+", " ").split()
                        entries = [e for e in entries if all(w in e["name"].lower() for w in words)]
                    skip = int(params.get("skip") or 0)
                    metas = [self._umeta(e, user_config) for e in entries[skip:skip + 100]]
                    return self._send(200, json.dumps({"metas": metas}).encode(), "application/json", True)
                return self._send(200, json.dumps({"metas": []}).encode(), "application/json", True)
            user_config, clean_path = self._extract_addon_config(path)
            if clean_path.startswith("/meta/tv/"):
                seg = urllib.parse.unquote(clean_path.rsplit("/", 1)[1].removesuffix(".json"))
                source, _, cid = seg.partition(":")
                if source == "u":
                    e = _unified_registry().get(_unb64u(cid))
                    if not e:
                        return self._send(200, json.dumps({"meta": {}}).encode(), "application/json")
                    return self._send(200, json.dumps({"meta": self._umeta(e, user_config)}).encode(),
                        "application/json", True)
                if source == "custom":
                    st = _settings.get("stremio", {})
                    cc = st.get("custom_channels", {}).get(cid, {})
                    c = {"id": cid, "name": cc.get("name", "Custom"), "logo": cc.get("logo", "")}
                    return self._send(200, json.dumps({"meta": self._meta(c, "custom", user_config)}).encode(),
                        "application/json", True)
                return self._send(200, json.dumps({"meta": {}}).encode(), "application/json")
            if clean_path.startswith("/stream/tv/"):
                seg = urllib.parse.unquote(clean_path.rsplit("/", 1)[1].removesuffix(".json"))
                source, _, cid = seg.partition(":")
                b = self._self_base()
                if source == "u":                 # chaîne unifiée -> TOUS les flux (toutes sources)
                    streams = unified_streams(_unb64u(cid), b)
                    return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")
                if source == "custom":            # chaîne perso (catalogue ⭐ Mes chaînes)
                    st = _settings.get("stremio", {})
                    cc = st.get("custom_channels", {}).get(cid, {})
                    cq, _r = _quality_of(cc.get("name", ""))
                    streams = [_stream_entry("⭐", cc.get("name", "Ma chaîne"), cq, f"Source {idx + 1}",
                        f"{b}/hls/custom/{cid}/s{idx}/index.m3u8", binge=f"cu-{cid}")
                        for idx, _u in enumerate(cc.get("streams", []))]
                    return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")
                return self._send(200, json.dumps({"streams": []}).encode(), "application/json")
            if path.startswith("/hls/") and path.endswith("/index.m3u8"):
                parts = path.split("/")
                if parts[2] == "custom":
                    cid = parts[3]
                    seg4 = parts[4] if len(parts) == 6 else ""
                    if seg4.startswith("s") and seg4[1:].isdigit():
                        idx = int(seg4[1:])
                        st = _settings.get("stremio", {})
                        custom_channels = st.get("custom_channels", {})
                        cc = custom_channels.get(cid, {})
                        streams = cc.get("streams", [])
                        if idx >= len(streams):
                            return self._send(404, b"flux custom inconnu", "text/plain")
                        m3u8 = streams[idx]
                        host = urllib.parse.urlsplit(m3u8).netloc
                        _track_play("custom", cid)
                        hdr = {"Referer": host + "/", "Origin": host}
                        henc = _b64u(json.dumps(hdr))
                        text = _proxy_get(m3u8, hdr).decode("utf-8", "replace")
                        return self._send(200, _rewrite_playlist(text, m3u8, henc, self._self_base()).encode(),
                            "application/vnd.apple.mpegurl")
                    return self._send(404, b"format custom invalide", "text/plain")
                cid = parts[2]
                st = _settings.get("stremio", {})
                seg3 = parts[3] if len(parts) == 5 else ""
                if seg3.startswith("custom_") and seg3[len("custom_"):].isdigit():
                    cs = st.get("channel_streams", {}).get(cid, [])
                    idx = int(seg3[len("custom_"):])
                    if idx >= len(cs):
                        return self._send(404, b"flux perso inconnu", "text/plain")
                    item = cs[idx]
                    m3u8 = item["url"] if isinstance(item, dict) else str(item)
                    host = urllib.parse.urlsplit(m3u8).netloc
                elif seg3.startswith("p") and seg3[1:].isdigit():
                    pls = players(cid)
                    idx = int(seg3[1:])
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
            if path == "/vghls":
                # VegetaTv : résout (load-balance byte-testé) -> URL MFP, et sert le manifeste MFP
                # BRUT (ses segments pointent sur MFP, qui DOIT donc être joignable par le client ->
                # MEDIAFLOW_URL = domaine PUBLIC, pas loopback). Loggé pour diagnostiquer une lecture KO.
                key = _unb64u(qs["v"][0])
                url = vegetatv_resolve(key)
                if not url:
                    log.warning(f"vghls: resolve VIDE pour {key!r} (toutes les lignes contendues au byte-test)")
                    return self._send(502, b"vegetatv: flux introuvable (lignes contendues, reessaie)", "text/plain")
                _track_play("vegetatv", key)
                try:
                    text = _proxy_get(url, {"User-Agent": _VEGETA_UA}).decode("utf-8", "replace")
                except Exception as ex:
                    log.error(f"vghls: MFP injoignable ({type(ex).__name__}) — MEDIAFLOW_URL={MEDIAFLOW_URL!r} depuis le conteneur ?")
                    return self._send(502, b"vegetatv: MFP injoignable (MEDIAFLOW_URL depuis le conteneur ?)", "text/plain")
                if "#EXTM3U" not in text:
                    log.warning(f"vghls: MFP a repondu sans manifeste HLS (api_password faux ? source morte ?) : {text[:120]!r}")
                return self._send(200, text.encode(), "application/vnd.apple.mpegurl")
            if path == "/px":
                ub = qs["u"][0]
                henc = qs.get("h", [""])[0]
                if not _proxy_ok(ub, henc, qs.get("s", [""])[0]):
                    return self._send(403, b"forbidden", "text/plain")
                url = _unb64u(ub)
                hdr = json.loads(_unb64u(henc)) if henc else {}
                text = _proxy_get(url, hdr).decode("utf-8", "replace")
                return self._send(200, _rewrite_playlist(text, url, henc, self._self_base()).encode(),
                    "application/vnd.apple.mpegurl")
            if path == "/sx":
                ub = qs["u"][0]
                henc = qs.get("h", [""])[0]
                if not _proxy_ok(ub, henc, qs.get("s", [""])[0]):
                    return self._send(403, b"forbidden", "text/plain")
                url = _unb64u(ub)
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
    def _manifest(self, lang_filter: str | None = None, user_config: dict | None = None) -> dict:
        uc = user_config or {}
        lang_filter = uc.get("lang", lang_filter or "fr")
        _extra = [{"name": "search", "isRequired": False},
            {"name": "skip", "isRequired": False},
            {"name": "genre", "isRequired": False,
                "options": _GENRE_CHOICES}]
        st = _settings.get("stremio", {})
        name = st.get("manifest_name") or "W Addon TV"
        desc = st.get("manifest_desc") or ("Chaînes TV en direct (sport, info, divertissement) via dlstreams + Vavoo, "
            "lues directement dans Stremio grâce au proxy intégré. Dashboard inclus.")
        lang_names = {"fr": "Français", "en": "English", "es": "Español", "de": "Deutsch", "it": "Italiano", "ar": "Arabe", "pt": "Português"}
        lang_name = lang_names.get(lang_filter, lang_filter)
        # Branding « W Addon TV » par défaut, avec les catégories en catalogues à l'intérieur.
        if not st.get("manifest_desc"):
            desc = "W Addon TV — chaînes FR unifiées (dlstreams + Vavoo + VegetaTv) rangées par catégorie, lues dans Stremio."
        # UN SEUL catalogue au nom de l'addon (« W Addon TV »), catégories en menu déroulant (genre)
        # — façon tvmio : l'utilisateur voit son addon + un sélecteur Sport/Cinéma/Info… à l'intérieur.
        cats = unified_categories()
        catalogs = [{"type": "tv", "id": "waddontv", "name": name,
            "extra": [{"name": "genre", "isRequired": False, "options": cats},
                      {"name": "search", "isRequired": False},
                      {"name": "skip", "isRequired": False}],
            "extraSupported": ["genre", "search", "skip"]}]
        custom_channels = st.get("custom_channels", {})
        if custom_channels:
            catalogs.append({"type": "tv", "id": "custom", "name": "⭐ Mes chaînes",
                "extra": [{"name": "search", "isRequired": False}, {"name": "skip", "isRequired": False}],
                "extraSupported": ["search", "skip"]})
        behavior = {
            "configurable": True,
            "configurationRequired": False,
            "adultContent": uc.get("adult", False),
            "p2p": False
        }
        return {
            "id": "st.waddontv.proxy.fr",
            "version": _VERSION,
            "name": name,
            "description": desc,
            "resources": ["catalog", "meta", "stream"],
            "types": ["tv"],
            "idPrefixes": ["u:", "custom:"],
            "catalogs": catalogs,
            "behaviorHints": behavior
        }
    def _umeta(self, e: dict, user_config: dict | None = None) -> dict:
        """Meta d'une chaîne UNIFIÉE : nom propre, logo curé, catégorie, résumé des sources/qualités."""
        base = self._self_base()
        cid = _b64u(e["key"])
        srcs = sorted({r["src"] for r in e["refs"]})
        src_lbl = {"dlstreams": "dlstreams", "vavoo": "Vavoo", "vegetatv": "Vegeta TV"}
        quals = list(dict.fromkeys(r["q"] for r in e["refs"] if r["q"]))
        poster = f"{base}/logo/unified/{urllib.parse.quote(cid, safe='')}.png"
        desc = (f"{e['name']} — {len(e['refs'])} flux "
            f"({', '.join(src_lbl.get(s, s) for s in srcs)})"
            + (f" · {'/'.join(quals)}" if quals else "") + ".")
        return {"id": f"u:{cid}", "type": "tv", "name": e["name"], "poster": poster, "logo": poster,
            "posterShape": "landscape", "background": poster, "description": desc,
            "releaseInfo": "En direct", "genres": [e["cat"]]}

    def _meta(self, c: dict, source: str, user_config: dict | None = None) -> dict:
        uc = user_config or {}
        raw_id = c["id"]
        if source in ("vavoo", "vegetatv"):
            cid = _b64u(raw_id)
        elif source == "dlstreams":
            cid = raw_id
        else:
            cid = raw_id
        base = self._self_base()
        st = _settings.get("stremio", {})
        show_logos = uc.get("logos", _settings.get("logos", True))
        if source == "custom":
            cc = st.get("custom_channels", {}).get(cid, {})
            display_name = cc.get("name", c["name"])
            custom_logo = cc.get("logo", "")
            if show_logos and custom_logo:
                poster = custom_logo
            else:
                poster = f"{base}/poster/{urllib.parse.quote(c['name'], safe='')}.png"
            lang = c.get("lang", uc.get("lang", "fr"))
            genres = _genres_for(c["name"])
            desc = f"Chaîne personnalisée {display_name} via proxy intégré."
            release = "En direct"
            return {"id": f"custom:{cid}", "type": "tv", "name": display_name,
                "poster": poster, "logo": poster, "posterShape": "landscape",
                "background": poster,
                "description": desc,
                "releaseInfo": release,
                "genres": genres}
        custom_name = st.get("channel_names", {}).get(str(raw_id))
        display_name = custom_name if custom_name else c["name"]
        custom_logo = st.get("channel_logos", {}).get(str(raw_id))
        if show_logos and custom_logo:
            poster = custom_logo
        elif show_logos:
            poster = f"{base}/logo/{source}/{urllib.parse.quote(cid, safe='')}.png"
        else:
            poster = f"{base}/poster/{urllib.parse.quote(c['name'], safe='')}.png"
        lang = c.get("lang", uc.get("lang", "fr"))
        lang_label = {"fr": "française", "en": "anglaise", "es": "espagnole",
            "de": "allemande", "it": "italienne", "ar": "arabe",
            "pt": "portugaise"}.get(lang, lang)
        genres = _genres_for(c["name"])
        desc = (f"Chaîne {display_name} diffusée en direct, chaîne {lang_label} "
            f"disponible via {source}. Lecture directe dans Stremio grâce au proxy intégré.")
        release = "En direct"
        if source == "dlstreams":
            st = _settings.get("stremio", {})
            custom_epg_id = st.get("channel_epg", {}).get(str(raw_id))
            epg_lookup_id = custom_epg_id if custom_epg_id else raw_id
            if uc.get("epg", _settings.get("epg", True)):
                cur, nxt = _epg_slot(epg_lookup_id)
                if cur:
                    t0 = time.strftime("%H:%M", time.localtime(cur["start"]))
                    t1 = time.strftime("%H:%M", time.localtime(cur["stop"]))
                    release = f"En direct · {t0}-{t1}"
                    desc = cur["title"] or "Programme en cours"
                    if cur.get("desc"):
                        desc += f"\n{cur['desc']}"
                    if nxt:
                        tn = time.strftime("%H:%M", time.localtime(nxt["start"]))
                        desc += f"\nÀ suivre à {tn} : {nxt['title']}"
                    desc += f"\nChaîne {display_name} diffusée en direct via {source}."
        return {"id": f"{source}:{cid}", "type": "tv", "name": display_name,
            "poster": poster, "logo": poster, "posterShape": "landscape",
            "background": poster,
            "description": desc,
            "releaseInfo": release,
            "genres": genres}

def main():
    _hist_load()
    _sessions_load()
    _settings_load()
    _epg_load()
    _playlists_load()
    _tokens_load()
    log.info(f"dlstreams addon+proxy sur http://0.0.0.0:{PORT}")
    log.info(f"  Dashboard: http://127.0.0.1:{PORT}/dashboard")
    log.info(f"  Configure: http://127.0.0.1:{PORT}/configure")
    log.info(f"  Stremio  : http://<ton-ip-LAN>:{PORT}/manifest.json?lang=fr")
    log.info(f"  VLC/mpv  : http://127.0.0.1:{PORT}/hls/121/index.m3u8")
    if _PASSWORD_GENERATED:
        log.info("=" * 62)
        log.info(f"Mot de passe dashboard (genere automatiquement): {DASHBOARD_PASSWORD}")
        log.info("Il changera au prochain redemarrage. Definis la variable DASHBOARD_PASSWORD sur Render pour en garder un stable.")
        log.info("=" * 62)
    else:
        log.info("Mot de passe dashboard : defini via DASHBOARD_PASSWORD")
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
    threading.Thread(target=_warm_logos, daemon=True).start()
    threading.Thread(target=_epg_refresh, daemon=True).start()
    threading.Thread(target=_health_refresh, daemon=True).start()
    if MEDIAFLOW_URL:                       # VegetaTv seulement si MFP est configuré
        threading.Thread(target=_vegeta_warm, daemon=True).start()
    srv.serve_forever()

def _warm_channels():
    try:
        n = len(channels())
        log.info(f"annuaire: {n} chaines chargees (dont {len(_POPULAR_CHANNELS)} populaires)")
    except Exception as e:
        log.error(f"annuaire: erreur de chargement ({e})")

if __name__ == "__main__":
    main()
