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

_CH_LOGO = {
    "121": "https://static.epg.best/fr/CanalPlus.fr.png",
    "122": "https://static.epg.best/fr/CanalPlusSport.fr.png",
    "123": "https://static.epg.best/fr/CanalPlusCinema.fr.png",
    "124": "https://static.epg.best/fr/CanalPlusSeries.fr.png",
    "125": "https://static.epg.best/fr/CanalPlusFamily.fr.png",
    "211": "https://static.epg.best/fr/RMCSport1.fr.png",
    "212": "https://static.epg.best/fr/RMCSport2.fr.png",
    "213": "https://static.epg.best/fr/RMCSport3.fr.png",
    "214": "https://static.epg.best/fr/RMCSport4.fr.png",
    "301": "https://static.epg.best/fr/Eurosport1.fr.png",
    "302": "https://static.epg.best/fr/Eurosport2.fr.png",
    "401": "https://static.epg.best/fr/TF1.fr.png",
    "402": "https://static.epg.best/fr/France2.fr.png",
    "403": "https://static.epg.best/fr/France3.fr.png",
    "404": "https://static.epg.best/fr/France4.fr.png",
    "405": "https://static.epg.best/fr/France5.fr.png",
    "406": "https://static.epg.best/fr/M6.fr.png",
    "407": "https://static.epg.best/fr/Arte.fr.png",
    "408": "https://static.epg.best/fr/C8.fr.png",
    "409": "https://static.epg.best/fr/W9.fr.png",
    "410": "https://static.epg.best/fr/TMC.fr.png",
    "411": "https://static.epg.best/fr/TFX.fr.png",
    "412": "https://static.epg.best/fr/NRJ12.fr.png",
    "413": "https://static.epg.best/fr/LCP.fr.png",
    "414": "https://static.epg.best/fr/FranceInfo.fr.png",
    "415": "https://static.epg.best/fr/BFMTV.fr.png",
    "416": "https://static.epg.best/fr/CNews.fr.png",
    "417": "https://static.epg.best/fr/CStar.fr.png",
    "418": "https://static.epg.best/fr/Gulli.fr.png",
    "419": "https://static.epg.best/fr/TF1SeriesFilms.fr.png",
    "420": "https://static.epg.best/fr/LEquipe.fr.png",
    "421": "https://static.epg.best/fr/6ter.fr.png",
    "422": "https://static.epg.best/fr/RMCStory.fr.png",
    "423": "https://static.epg.best/fr/RMCDecouverte.fr.png",
    "424": "https://static.epg.best/fr/Cherie25.fr.png",
    "201": "https://static.epg.best/fr/BeinSports1.fr.png",
    "202": "https://static.epg.best/fr/BeinSports2.fr.png",
    "203": "https://static.epg.best/fr/BeinSports3.fr.png",
    "116": "https://static.epg.best/fr/BeinSports1.fr.png",
    "772": "https://static.epg.best/fr/Eurosport1.fr.png",
    "645": "https://static.epg.best/fr/LEquipe.fr.png",
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
        "default_lang": "fr",
        "channel_names": {},
        "channel_logos": {},
        "channel_streams": {},
        "channel_epg": {},
        "custom_channels": {},
    },
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
    "121": "CanalPlus.fr", "122": "CanalPlusSport.fr", "123": "CanalPlusCinema.fr",
    "124": "CanalPlusSeries.fr", "125": "CanalPlusFamilyCentre.af",
    "201": "beINSPORTS1.fr", "202": "beINSPORTS2.fr", "203": "beINSPORTS3.fr",
    "211": "RMCSport1.fr", "212": "RMCSport2.fr", "213": "RMCSport3.fr", "214": "RMCSport4.fr",
    "301": "Eurosport1.fr", "302": "Eurosport2.fr",
    "401": "TF1.fr", "402": "France2.fr", "403": "France3.fr", "404": "France4.fr",
    "405": "France5.fr", "406": "M6.fr", "407": "Arte.fr", "409": "W9.fr", "410": "TMC.fr",
    "413": "LCP100.fr", "414": "FranceInfo.fr", "415": "BFMTV.fr", "416": "CNews.fr",
    "417": "CStar.fr", "418": "Gulli.fr", "419": "TF1SeriesFilms.fr", "420": "LEquipe21.fr",
    "421": "6ter.fr", "423": "RMCDecouverte.fr", "424": "Cherie25.fr",
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
    n = name.lower().strip()
    if any(k in n for k in ["sport", "foot", "tennis", "racing", "formula", "f1 racing", "golf", "cycl",
        "beinsport", "bein", "eurosport", "rmc sport", "canal+ sport", "ufc", "boxe",
        "mma", "wwe", "équipe", "equipe", "olymp", "auto moto"]):
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

_PROXY_SECRET = secrets.token_bytes(32)

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
    with _health_lock:
        _health_snapshot.update(
            at=time.time(),
            dlstreams=dl, vavoo=vv,
            epg={"ok": epg_ok, "channels": epg_channels, "age": epg_age},
            logos={"ok": logos_ok, "loaded": logos_loaded, "total": logos_total},
        )

def _now_playing() -> list[dict]:
    with _epg_lock:
        if not _epg_data:
            return []
        out = []
        for c in _POPULAR_CHANNELS:
            cur, nxt = _epg_slot(c["id"])
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

def _logo_bytes(src: str, c: dict) -> bytes:
    if not _settings.get("logos", True):
        return _poster_get(c.get("name") or "TV")
    name = c.get("name") or ""
    cid = str(c.get("id") or "")
    st = _settings.get("stremio", {})
    manual = st.get("channel_logos", {}).get(cid)
    if manual:
        url = manual.strip()
    elif src == "dlstreams":
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

def _load_html_files():
    global DASHBOARD_HTML, CONFIGURE_HTML
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
        if path == "/api/health":
            if not self._require_auth():
                return
            _health_refresh(force=True)
            return self._send(200, json.dumps(_health_snapshot).encode(), "application/json")
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
                else:
                    c = next((x for x in channels() if str(x.get("id")) == str(cid)),
                        {"id": cid, "name": f"dlstreams {cid}", "logo": ""})
                return self._send(200, _logo_bytes(src, c), "image/png", True)
            if path.startswith("/poster/") and path.endswith(".png"):
                pname = urllib.parse.unquote(path[len("/poster/"):-4])
                return self._send(200, _poster_get(pname), "image/png", True)
            if path == "/dashboard" or path == "/dashboard.html" or path.startswith("/dashboard/"):
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
            if path == "/api/public-stats":
                return self._send(200, json.dumps(_public_stats()).encode(), "application/json", True)
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
                _health_refresh(force=True)
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
            if path.startswith("/") and path.endswith("/manifest.json"):
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
                self.end_headers()
                self.wfile.write(body)
                return
            if path in ("/", "/manifest.json"):
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
                if catid == "dlstreams":
                    if not user_config.get("dlstreams", True):
                        return self._send(200, json.dumps({"metas": []}).encode(), "application/json", True)
                    chans = channels(lang_filter=lang_filter)
                elif catid == "vavoo":
                    if not user_config.get("vavoo", True):
                        return self._send(200, json.dumps({"metas": []}).encode(), "application/json", True)
                    chans = [c for c in vavoo_channels() if c.get("lang") == lang_filter]
                else:
                    chans = channels(lang_filter=lang_filter)
                q = params.get("search", "").lower().strip()
                if q:
                    words = q.replace("+", " ").split()
                    chans = [c for c in chans if all(w in c["name"].lower() for w in words)]
                g = params.get("genre")
                if g:
                    chans = [c for c in chans if g in _genres_for(c["name"])]
                skip = int(params.get("skip") or 0)
                metas = [self._meta(c, catid, user_config) for c in chans[skip:skip + 100]]
                return self._send(200, json.dumps({"metas": metas}).encode(), "application/json", True)
            user_config, clean_path = self._extract_addon_config(path)
            if clean_path.startswith("/meta/tv/"):
                seg = urllib.parse.unquote(clean_path.rsplit("/", 1)[1].removesuffix(".json"))
                source, _, cid = seg.partition(":")
                if source == "vavoo":
                    url = _unb64u(cid)
                    c = next((x for x in vavoo_channels() if x["id"] == url), {"id": url, "name": "Vavoo"})
                else:
                    c = next((x for x in channels() if x["id"] == cid), {"id": cid, "name": f"dlstreams {cid}"})
                return self._send(200, json.dumps({"meta": self._meta(c, source, user_config)}).encode(),
                    "application/json", True)
            if clean_path.startswith("/stream/tv/"):
                seg = urllib.parse.unquote(clean_path.rsplit("/", 1)[1].removesuffix(".json"))
                source, _, cid = seg.partition(":")
                b = self._self_base()
                if source == "vavoo":
                    if not user_config.get("vavoo", True):
                        return self._send(200, json.dumps({"streams": []}).encode(), "application/json")
                    streams = [{"name": "Vavoo", "title": "📺 Direct", "url": f"{b}/vhls?v={cid}"}]
                    return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")
                st = _settings.get("stremio", {})
                streams = []
                custom_channels = st.get("custom_channels", {})
                if cid in custom_channels:
                    cc = custom_channels[cid]
                    for idx, stream_url in enumerate(cc.get("streams", [])):
                        streams.append({"name": "Personnalisé", "title": f"{cc.get('name', 'Custom')}  Source {idx+1}",
                            "url": f"{b}/hls/custom/{cid}/s{idx}/index.m3u8"})
                    if streams:
                        return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")
                custom_streams = st.get("channel_streams", {}).get(cid, [])
                if isinstance(custom_streams, str):
                    custom_streams = [{"url": custom_streams, "label": ""}]
                for idx, sitem in enumerate(custom_streams):
                    label = (sitem.get("label") if isinstance(sitem, dict) else "") or f"Flux perso {idx+1}"
                    streams.append({"name": "Personnalisé", "title": f"⚙️ {label}",
                        "url": f"{b}/hls/{cid}/custom_{idx}/index.m3u8"})
                if user_config.get("dlstreams", True):
                    ok = working_players(cid)
                    if ok:
                        quality_pref = user_config.get("quality", "auto")
                        streams.append({"name": "dlstreams", "title": "🔀 Auto (1er dispo)",
                            "url": f"{b}/hls/{cid}/index.m3u8"})
                        for idx, (i, label) in enumerate(ok):
                            title = f"Source {idx+1}"
                            if quality_pref != "auto":
                                title = f"{quality_pref} {title}"
                            streams.append({"name": "dlstreams", "title": title,
                                "url": f"{b}/hls/{cid}/p{i}/index.m3u8"})
                return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")
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
        if not st.get("manifest_name"):
            name = f"Chaînes live {lang_name}"
        if not st.get("manifest_desc"):
            desc = f"Chaînes TV en direct en {lang_name} (dlstreams + Vavoo), lues directement dans Stremio via le proxy intégré."
        catalogs = []
        if uc.get("dlstreams", True) and st.get("include_dlstreams", True):
            catalogs.append({"type": "tv", "id": "dlstreams", "name": "W Addon TV",
                "extra": _extra, "extraSupported": ["search", "skip", "genre"]})
        if uc.get("vavoo", True) and st.get("include_vavoo", True):
            catalogs.append({"type": "tv", "id": "vavoo", "name": "Vavoo",
                "extra": _extra, "extraSupported": ["search", "skip", "genre"]})
        custom_channels = st.get("custom_channels", {})
        if custom_channels:
            catalogs.append({"type": "tv", "id": "custom", "name": "⭐ Mes chaînes",
                "extra": _extra, "extraSupported": ["search", "skip"]})
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
            "idPrefixes": ["dlstreams:", "vavoo:", "custom:"],
            "catalogs": catalogs,
            "behaviorHints": behavior
        }
    def _meta(self, c: dict, source: str, user_config: dict | None = None) -> dict:
        uc = user_config or {}
        raw_id = c["id"]
        if source == "vavoo":
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
    srv.serve_forever()

def _warm_channels():
    try:
        n = len(channels())
        log.info(f"annuaire: {n} chaines chargees (dont {len(_POPULAR_CHANNELS)} populaires)")
    except Exception as e:
        log.error(f"annuaire: erreur de chargement ({e})")

if __name__ == "__main__":
    main()