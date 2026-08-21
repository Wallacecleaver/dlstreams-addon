#!/usr/bin/env python3
"""dlstreams -> Stremio : mini-addon + proxy autonome avec dashboard complet.
Dashboard avec session persistante, gestion des sources, et navigation SPA.
"""
from __future__ import annotations
import base64
import gzip
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
import xml.sax
import zlib
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8781"))
_VERSION = "1.13.3"

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
_hist_err: list[list[int]] = []   # [minute_unix, nb_erreurs] -> courbe des erreurs
_live: dict[str, float] = {}      # cle "src:id" -> dernier timestamp de lecture (fenetre courte)
_LIVE_WINDOW = 180                # secondes : une lecture est "en cours" pendant 3 min
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

# Logos reels des chaines populaires FR (ids dlstreams -> URL logo).
# Sources : static.epg.best (EPG/logo iptv-org) + Wikimedia. Servees via le
# proxy /logo/... avec fallback poster genere si indisponible.
_CH_LOGO = {
    "121": "https://static.epg.best/fr/CanalPlus.fr.png",
    "122": "https://static.epg.best/fr/CanalPlusSport.fr.png",
    "123": "https://upload.wikimedia.org/wikipedia/fr/thumb/e/eb/C%2B_Cin%C3%A9ma%28s%29.png/500px-C%2B_Cin%C3%A9ma%28s%29.png",
    "124": "https://upload.wikimedia.org/wikipedia/fr/thumb/e/e3/C%2B_S%C3%A9ries.png/500px-C%2B_S%C3%A9ries.png",
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
    "407": "https://upload.wikimedia.org/wikipedia/commons/thumb/4/43/Arte_Logo_2017.svg/500px-Arte_Logo_2017.svg.png",
    "408": "https://static.epg.best/fr/C8.fr.png",
    "409": "https://static.epg.best/fr/W9.fr.png",
    "410": "https://static.epg.best/fr/TMC.fr.png",
    "411": "https://static.epg.best/fr/TFX.fr.png",
    "412": "https://static.epg.best/fr/NRJ12.fr.png",
    "413": "https://static.epg.best/fr/LCP.fr.png",
    "415": "https://static.epg.best/fr/BFMTV.fr.png",
    "416": "https://static.epg.best/fr/CNews.fr.png",
    "417": "https://static.epg.best/fr/CStar.fr.png",
    "418": "https://static.epg.best/fr/Gulli.fr.png",
    "419": "https://static.epg.best/fr/TF1SeriesFilms.fr.png",
    "421": "https://static.epg.best/fr/6ter.fr.png",
    "422": "https://static.epg.best/fr/RMCStory.fr.png",
    "423": "https://commons.wikimedia.org/wiki/Special:FilePath/Logo_de_RMC_d%C3%A9couverte_depuis_le_08-11-2025.png",
    "424": "https://static.epg.best/fr/Cherie25.fr.png",
    "414": "https://static.epg.best/fr/FranceInfo.fr.png",
    "420": "https://static.epg.best/fr/LEquipe21.fr.png",
    "645": "https://static.epg.best/fr/LEquipe21.fr.png",
    "201": "https://static.epg.best/fr/BeinSports1.fr.png",
    "202": "https://static.epg.best/fr/BeinSports2.fr.png",
    "203": "https://static.epg.best/fr/BeinSports3.fr.png",
    "116": "https://static.epg.best/fr/BeinSports1.fr.png",
    "772": "https://static.epg.best/fr/Eurosport1.fr.png",
    "960": "https://commons.wikimedia.org/wiki/Special:FilePath/Ligue1%20logo.png",
    "68": "https://commons.wikimedia.org/wiki/Special:FilePath/Ligue1%20logo.png",
    "76": "https://commons.wikimedia.org/wiki/Special:FilePath/Ligue1%20logo.png",
}

# ============================ REGLAGES ============================
_SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlstreams_settings.json")
_SETTINGS_DEFAULT = {
    "logos": True,                                   # logos reels des chaines populaires
    "epg": True,                                     # EPG (programme en cours)
    "epg_url": "https://xmltvfr.fr/xmltv/xmltv.xml.gz",
    "genres": {},                                    # overrides: nom chaine (minuscule) -> [genres]
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
    },
}
_settings: dict = dict(_SETTINGS_DEFAULT)

def _settings_save():
    try:
        with open(_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(_settings, f, ensure_ascii=False, indent=1)
    except Exception:
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
    except Exception:
        pass

# ============================ EPG (programme TV) ============================
# ids dlstreams -> ids XMLTV (xmltvfr.fr utilise les ids iptv-org, memes que les logos)
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
    except Exception:
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
    except Exception:
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
    except Exception:
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
        print(f"  epg : {len(_epg_data)} chaines guidees ({len(handler.out)} programmes)")
    except Exception as e:
        print(f"  epg : erreur ({type(e).__name__}: {e})")
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp.name)
            except Exception:
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

# ============================ CATEGORIES ============================
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
    except Exception:
        pass

def _hist_save():
    """Persiste l'historique sur disque (appele une fois par minute)."""
    try:
        with open(_HIST_FILE, "w", encoding="utf-8") as f:
            json.dump({"req": _hist, "err": _hist_err}, f)
    except Exception:
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
    """Lectures considerees 'en cours' (dernier acces < 3 min)."""
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
        for ch in _ch_cache["list"]:
            lg = _CH_LOGO.get(str(ch.get("id")))
            if lg:
                ch["logo"] = lg

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
        "hist_err": [list(pair) for pair in _hist_err],
        "health": _health_snapshot,
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

_logo_cache: dict[str, bytes] = {}
_logo_bad: set[str] = set()

# --- Sante des services (carte "Etat des services" du dashboard) ---
_health_snapshot: dict = {"at": 0.0}
_health_lock = threading.Lock()
_HEALTH_TTL = 60

def _health_refresh(force: bool = False):
    """Verifie dlstreams.st et l'API vavoo (en parallele), plus l'etat EPG/logos."""
    with _health_lock:
        if not force and time.time() - _health_snapshot.get("at", 0) < _HEALTH_TTL:
            return
    def _dl():
        t0 = time.time()
        try:
            _get(SITE + "/", timeout=8)
            return {"ok": True, "ms": int((time.time() - t0) * 1000)}
        except Exception:
            return {"ok": False, "ms": int((time.time() - t0) * 1000)}
    def _vv():
        t0 = time.time()
        try:
            _post_json(_VAVOO_PINGS[0], _vavoo_ping_body(), {"user-agent": _VAVOO_UA}, timeout=8)
            return {"ok": True, "ms": int((time.time() - t0) * 1000)}
        except Exception:
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
    """Programme en cours des chaines populaires (mini-EPG du dashboard)."""
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
    """Logo reel (proxye) si dispo, sinon poster genere. Jamais de tile cassee."""
    if not _settings.get("logos", True):
        return _poster_get(c.get("name") or "TV")
    url = _CH_LOGO.get(str(c.get("id")), "") if src == "dlstreams" else (c.get("logo") or "").strip()
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
    return _poster_get(c.get("name") or "TV")

def _warm_logos():
    for url in _CH_LOGO.values():
        # prefetch des logos populaires pour que les posters soient instantanes
        try:
            if url not in _logo_cache and url not in _logo_bad:
                req = urllib.request.Request(url, headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=15) as r:
                    b = r.read()
                if b[:3] in (b"\xff\xd8\xff", b"\x89PN") and len(_logo_cache) < 400:
                    _logo_cache[url] = b
        except Exception:
            _logo_bad.add(url)

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

        if path == "/api/settings":
            if not self._require_auth():
                return
            try:
                data = json.loads(body) if body else {}
            except Exception:
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
                for k in ("channel_names", "channel_logos", "channel_streams", "channel_epg"):
                    if isinstance(st.get(k), dict):
                        _settings["stremio"][k] = {str(kk): str(vv) for kk, vv in st[k].items()}
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
                    c = next((x for x in vavoo_channels() if x["id"] == url),
                             {"id": url, "name": "Vavoo", "logo": ""})
                else:
                    c = next((x for x in channels() if str(x.get("id")) == str(cid)),
                             {"id": cid, "name": f"dlstreams {cid}", "logo": ""})
                return self._send(200, _logo_bytes(src, c), "image/png", True)

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
                    rest = extra.split("/", 1)[1]
                    if rest.startswith("genre/"):
                        params["genre"] = urllib.parse.unquote_plus(rest[len("genre/"):])
                    else:
                        for kv in rest.split("&"):
                            if "=" in kv:
                                k, v = kv.split("=", 1)
                                params[k] = urllib.parse.unquote_plus(v)

                lang_filter = qs.get("lang", [None])[0]

                chans = vavoo_channels() if catid == "vavoo" else channels(lang_filter=lang_filter)
                q = params.get("search", "").lower().strip()
                if q:
                    words = q.replace("+", " ").split()
                    chans = [c for c in chans if all(w in c["name"].lower() for w in words)]
                g = params.get("genre")
                if g:
                    chans = [c for c in chans if g in _genres_for(c["name"])]
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
                # Check for custom stream
                st = _settings.get("stremio", {})
                custom_stream = st.get("channel_streams", {}).get(cid)
                ok = working_players(cid)
                streams = []
                if custom_stream:
                    streams.append({"name": "Personnalisé", "title": "⚙️ Flux personnalisé",
                                "url": f"{b}/hls/{cid}/index.m3u8"})
                if ok:
                    streams.append({"name": "dlstreams", "title": "🔀 Auto (1er dispo)",
                                "url": f"{b}/hls/{cid}/index.m3u8"})
                    streams += [{"name": "dlstreams", "title": label,
                                "url": f"{b}/hls/{cid}/p{i}/index.m3u8"} for i, label in ok]
                return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")

            if path.startswith("/hls/") and path.endswith("/index.m3u8"):
                parts = path.split("/")
                cid = parts[2]
                # Check for custom stream in stremio settings
                st = _settings.get("stremio", {})
                custom_stream = st.get("channel_streams", {}).get(cid)
                if custom_stream:
                    m3u8 = custom_stream
                    host = urllib.parse.urlsplit(custom_stream).netloc
                elif len(parts) == 5 and parts[3].startswith("p") and parts[3][1:].isdigit():
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
            _extra = [{"name": "search", "isRequired": False},
                      {"name": "skip", "isRequired": False},
                      {"name": "genre", "isRequired": False,
                       "options": _GENRE_CHOICES}]

            st = _settings.get("stremio", {})
            name = st.get("manifest_name") or "Chaînes live (dlstreams + Vavoo)"
            desc = st.get("manifest_desc") or ("Chaînes TV en direct (sport, info, divertissement) via dlstreams + Vavoo, "
                    "lues directement dans Stremio grâce au proxy intégré. Dashboard inclus.")

            if lang_filter and lang_filter != "all":
                lang_names = {"fr": "Français", "en": "English", "es": "Español", "de": "Deutsch", "it": "Italiano", "ar": "Arabe", "pt": "Português"}
                lang_name = lang_names.get(lang_filter, lang_filter)
                if not st.get("manifest_name"):
                    name = f"Chaînes live {lang_name}"
                if not st.get("manifest_desc"):
                    desc = f"Chaînes TV en direct en {lang_name} (dlstreams + Vavoo), lues directement dans Stremio via le proxy intégré."

            catalogs = []
            if st.get("include_dlstreams", True):
                catalogs.append({"type": "tv", "id": "dlstreams", "name": "dlstreams",
                              "extra": _extra, "extraSupported": ["search", "skip", "genre"]})
            if st.get("include_vavoo", True):
                catalogs.append({"type": "tv", "id": "vavoo", "name": "Vavoo",
                              "extra": _extra, "extraSupported": ["search", "skip", "genre"]})

            return {
                "id": "st.dlstreams.proxy" + (f".{lang_filter}" if lang_filter and lang_filter != "all" else ""),
                "version": _VERSION,
                "name": name,
                "description": desc,
                "resources": ["catalog", "meta", "stream"],
                "types": ["tv"],
                "idPrefixes": ["dlstreams:", "vavoo:"],
                "catalogs": catalogs,
            }

    def _meta(self, c: dict, source: str) -> dict:
        cid = c["id"] if source == "dlstreams" else _b64u(c["id"])
        base = self._self_base()

        st = _settings.get("stremio", {})
        # Custom name
        custom_name = st.get("channel_names", {}).get(str(c["id"]))
        display_name = custom_name if custom_name else c["name"]

        # Custom logo
        custom_logo = st.get("channel_logos", {}).get(str(c["id"]))
        if _settings.get("logos", True) and custom_logo:
            poster = custom_logo
        elif _settings.get("logos", True):
            poster = f"{base}/logo/{source}/{urllib.parse.quote(cid, safe='')}.png"
        else:
            poster = f"{base}/poster/{urllib.parse.quote(c['name'], safe='')}.png"

        lang = c.get("lang", "fr")
        lang_label = {"fr": "française", "en": "anglaise", "es": "espagnole",
                      "de": "allemande", "it": "italienne", "ar": "arabe",
                      "pt": "portugaise"}.get(lang, lang)
        genres = _genres_for(c["name"])
        desc = (f"Chaîne {display_name} diffusée en direct, chaîne {lang_label} "
                f"disponible via {source}. Lecture directe dans Stremio grâce au proxy intégré.")
        release = "En direct"
        if source == "dlstreams":
            # Check for custom EPG ID override
            st = _settings.get("stremio", {})
            custom_epg_id = st.get("channel_epg", {}).get(str(c["id"]))
            epg_lookup_id = custom_epg_id if custom_epg_id else c["id"]
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
                    desc += f"\n\nÀ suivre à {tn} : {nxt['title']}"
                desc += f"\n\nChaîne {display_name} diffusée en direct via {source}."
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
    threading.Thread(target=_warm_logos, daemon=True).start()
    threading.Thread(target=_epg_refresh, daemon=True).start()
    threading.Thread(target=_health_refresh, daemon=True).start()
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
  .toggle-row { display:flex; align-items:center; justify-content:space-between; gap:14px; padding:12px 4px; cursor:pointer;
    border-bottom:1px solid var(--border); }
  .toggle-row:last-child { border-bottom:none; }
  .toggle-title { font-size:14px; font-weight:600; }
  .toggle-sub { font-size:12px; color:var(--text2); margin-top:2px; }
  .toggle-row input[type="checkbox"] { width:42px; height:24px; appearance:none; -webkit-appearance:none; background:var(--surface2);
    border:1px solid var(--border); border-radius:20px; position:relative; cursor:pointer; transition:background .2s; flex-shrink:0; }
  .toggle-row input[type="checkbox"]::after { content:""; position:absolute; top:2px; left:2px; width:18px; height:18px;
    border-radius:50%; background:var(--muted); transition:all .2s; }
  .toggle-row input[type="checkbox"]:checked { background:var(--accent-dim); }
  .toggle-row input[type="checkbox"]:checked::after { left:20px; background:var(--accent); }
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
  /* Sélecteur de source (segmented) */
  .tabs { display:flex; gap:4px; background:var(--surface2); border:1px solid var(--border); border-radius:10px; padding:3px; }
  .tab { border:none; background:transparent; color:var(--muted); font-size:12px; font-weight:800;
    padding:7px 16px; border-radius:8px; cursor:pointer; font-family:var(--font-body); transition:all .15s; letter-spacing:.2px; }
  .tab:hover { color:var(--text); }
  .tab.active { background:var(--accent); color:#fff; box-shadow:0 2px 10px rgba(229,62,62,.35); }

  /* Barre de recherche catalogue */
  .catalog-search { position:relative; flex:1; min-width:220px; }
  .catalog-search .search-ico { position:absolute; left:12px; top:50%; transform:translateY(-50%); font-size:13px; opacity:.55; pointer-events:none; }
  .catalog-search input[type="search"] { width:100%; background:var(--input-bg); border:1px solid var(--border);
    border-radius:10px; padding:10px 14px 10px 34px; color:var(--text); font-size:13px; font-family:var(--font-body); }
  .catalog-search input:focus { outline:none; border-color:var(--accent); }
  .catalog-subbar { display:flex; align-items:center; gap:10px; padding:10px 20px; border-bottom:1px solid var(--border);
    flex-wrap:wrap; background:linear-gradient(180deg, rgba(229,62,62,0.04), rgba(229,62,62,0)); }
  .catalog-subbar select { background:var(--input-bg); border:1px solid var(--border); border-radius:8px; padding:8px 12px;
    color:var(--text); cursor:pointer; font-family:var(--font-body); font-size:12px; font-weight:600; }
  .catalog-subbar select:focus { outline:none; border-color:var(--accent); }
  .catalog-subbar select:disabled { opacity:.45; cursor:not-allowed; }

  .channel-list { display:grid; grid-template-columns:repeat(auto-fill,minmax(170px,1fr)); gap:14px; }
  .list-count { font-size:12px; color:var(--muted); font-weight:600; }

  /* Cartes chaînes (vignette poster/logo + nom) */
  .chan-card { display:block; border-radius:14px; border:1px solid var(--border); background:var(--surface);
    overflow:hidden; text-decoration:none; color:var(--text); cursor:pointer;
    transition:transform .18s cubic-bezier(.22,1,.36,1), border-color .18s, box-shadow .18s; }
  .chan-card:hover { transform:translateY(-4px); border-color:rgba(229,62,62,.45); box-shadow:0 12px 30px rgba(0,0,0,.35); }
  .chan-tile { position:relative; aspect-ratio:16/9;
    background:radial-gradient(120% 140% at 20% 0%, #1d1d21 0%, var(--surface2) 60%, var(--surface) 100%);
    overflow:hidden; }
  .chan-logo { width:100%; height:100%; object-fit:contain; transition:transform .25s cubic-bezier(.22,1,.36,1); }
  .chan-card:hover .chan-logo { transform:scale(1.06); }
  .chan-play { position:absolute; inset:0; display:grid; place-items:center; background:rgba(10,10,11,0);
    opacity:0; transition:opacity .18s, background .18s; }
  .chan-card:hover .chan-play { opacity:1; background:rgba(10,10,11,0.55); }
  .play-badge { width:46px; height:46px; border-radius:50%; background:var(--accent); color:#fff; display:grid;
    place-items:center; font-size:16px; box-shadow:0 8px 24px rgba(229,62,62,.5); transform:scale(.8); transition:transform .18s; }
  .chan-card:hover .play-badge { transform:scale(1); }
  .chan-check { position:absolute; top:8px; left:8px; font-size:10px; font-weight:800; padding:3px 8px; border-radius:8px;
    background:rgba(0,0,0,.5); color:#aaa; cursor:pointer; z-index:2; font-family:var(--font-mono); transition:all .18s; letter-spacing:.3px; }
  .chan-check.ok { color:#48bb78; }
  .chan-check.ko { color:#ef4444; }
  .chan-check.busy { opacity:.6; pointer-events:none; }
  .chan-body { padding:11px 12px 13px; }
  .chan-name { font-size:13px; font-weight:700; color:var(--text); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .chan-meta { font-size:11px; color:var(--muted); margin-top:3px; font-family:var(--font-mono); }
  .fav-empty { grid-column:1/-1; text-align:center; padding:26px; color:var(--muted); font-size:13px; }
  .fav-empty a { color:var(--accent); }

/* Sources manuelles */
  .manual-channels-list { display:grid; grid-template-columns:repeat(auto-fill,minmax(260px,1fr)); gap:12px; margin-top:16px; }
  .manual-channel-card { display:flex; flex-direction:column; background:var(--surface2); border:1px solid var(--border);
    border-radius:12px; overflow:hidden; text-decoration:none; color:var(--text); transition:all .18s; }
  .manual-channel-card:hover { transform:translateY(-2px); border-color:var(--accent); box-shadow:0 10px 26px rgba(0,0,0,.35); }
  .manual-tile { position:relative; aspect-ratio:16/9; background:var(--surface3); display:grid; place-items:center; overflow:hidden; }
  .manual-logo { width:62%; max-height:62%; object-fit:contain; filter:drop-shadow(0 4px 12px rgba(0,0,0,.4)); }
  .manual-badge { position:absolute; top:8px; left:8px; display:flex; align-items:center; gap:5px; font-size:9px; font-weight:800;
    color:#fff; background:rgba(100,100,110,.9); padding:3px 9px; border-radius:999px; letter-spacing:.4px; }
  .manual-body { padding:11px 13px 13px; display:flex; flex-direction:column; gap:5px; flex:1; }
  .manual-name { font-size:13px; font-weight:700; color:var(--text); line-height:1.3;
    display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
  .manual-meta { font-size:11px; color:var(--muted); font-family:var(--font-mono); }
  .manual-actions { display:flex; gap:8px; margin-top:6px; padding-top:8px; border-top:1px solid var(--border); }
  .manual-actions button { flex:1; padding:8px 10px; border-radius:8px; font-size:12px; font-weight:600;
    border:none; cursor:pointer; transition:all .15s; }
  .manual-test { background:var(--accent); color:#fff; }
  .manual-test:hover { background:#c53030; }
  .manual-test.busy { opacity:.6; pointer-events:none; }
  .manual-del { background:rgba(239,68,68,.1); color:var(--error); border:1px solid var(--error); }
  .manual-del:hover { background:rgba(239,68,68,.2); }
  .add-source-box { background:var(--input-bg); border:2px dashed var(--border);
    border-radius:12px; padding:20px; margin-bottom:14px; transition:all .3s; }
  .add-source-box:hover { border-color:var(--accent); }
  .add-source-input { width:100%; background:var(--surface); border:1px solid var(--border);
    border-radius:10px; padding:12px 16px; color:var(--text);
    font-size:13px; font-family:var(--font-mono); margin-bottom:12px; transition:all .3s; }
  .add-source-input:focus { outline:none; border-color:var(--accent); }
  .add-source-btn { padding:12px 24px; background:var(--accent); border:none; border-radius:10px; color:#fff;
    font-weight:700; cursor:pointer; transition:all .2s; width:100%; }
  .add-source-btn:hover { background:#c53030; }
  .add-source-btn:disabled { opacity:.6; cursor:not-allowed; }
  .add-source-result { margin-top:12px; font-size:13px; }
  .add-source-preview { margin-top:12px; max-height:200px; overflow-y:auto; }
  .add-source-preview-item { padding:8px 10px; background:var(--surface2); border:1px solid var(--border); border-radius:8px;
    margin-bottom:6px; font-size:12px; display:flex; justify-content:space-between; gap:8px; }
  .add-source-preview-name { font-weight:600; color:var(--text); }
  .add-source-preview-id { color:var(--muted); font-family:var(--font-mono); }
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

  /* LOGS — cartes propres */
  .logs-toolbar { display:flex; align-items:center; gap:10px; flex-wrap:wrap; padding-bottom:8px; border-bottom:1px solid var(--border); }
  .logs-stats { display:flex; align-items:center; gap:12px; margin-right:auto; }
  .log-stat { font-size:11px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }
  .log-stat b { font-size:13px; color:var(--text); }
  .log-stat.ok b { color:var(--green); }
  .log-stat.warn b { color:var(--warn); }
  .log-stat.err b { color:var(--error); }
  .logs-group { display:flex; align-items:center; gap:6px; }
  .logs-group label { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.6px; font-weight:700; }
  .logs-select, .logs-search input { background:var(--surface); border:1px solid var(--border); border-radius:7px; padding:7px 10px;
    font-size:12px; font-weight:600; color:var(--text); font-family:var(--font-body); cursor:pointer; }
  .logs-search { position:relative; }
  .logs-search .search-ico { position:absolute; right:10px; top:50%; transform:translateY(-50%); opacity:.5; pointer-events:none; }
  .logs-search input { cursor:text; min-width:200px; font-weight:500; padding-right:32px; }
  .logs-search input:focus, .logs-select:focus { outline:none; border-color:var(--accent); }
  .logs-pausebtn { display:flex; align-items:center; gap:6px; background:var(--surface2); border:1px solid var(--border); border-radius:7px;
    padding:7px 12px; font-size:12px; font-weight:700; color:var(--text2); cursor:pointer; font-family:var(--font-body); transition:all .15s; }
  .logs-pausebtn:hover { color:var(--text); border-color:var(--text2); }
  .logs-pausebtn.paused { background:var(--accent-dim); border-color:rgba(229,62,62,0.3); color:var(--accent); }
  .logs-status { display:flex; align-items:center; gap:7px; font-size:11px; color:var(--muted); font-weight:700; text-transform:uppercase; letter-spacing:.5px; }
  .logs-dot { width:8px; height:8px; border-radius:50%; background:var(--green); animation:logsPulse 1.6s infinite; flex-shrink:0; }
  .logs-dot.paused { background:var(--muted); animation:none; }
  @keyframes logsPulse { 0%,100%{opacity:1} 50%{opacity:.35} }
  .logs-list { max-height:520px; overflow-y:auto; font-family:var(--font-mono); font-size:13px; }
  .logs-list::-webkit-scrollbar { width:8px; }
  .logs-list::-webkit-scrollbar-thumb { background:var(--border); border-radius:8px; }
  .log-entry { display:flex; align-items:center; gap:10px; padding:10px 16px; border-bottom:1px solid var(--border); transition:background .15s; }
  .log-entry:last-child { border-bottom:none; }
  .log-entry:hover { background:var(--card-hover); }
  .log-entry.warn { background:rgba(245,158,11,0.03); }
  .log-entry.err { background:rgba(230,57,70,0.04); }
  .log-time { font-size:13px; color:var(--muted); white-space:nowrap; flex-shrink:0; }
  .log-method { display:inline-flex; align-items:center; justify-content:center; min-width:56px; height:26px; border-radius:4px; font-size:11px; font-weight:800; letter-spacing:.3px; text-transform:uppercase; }
  .log-method.get { background:rgba(72,187,120,.15); color:var(--green); }
  .log-method.post { background:rgba(59,130,246,.15); color:#3b82f6; }
  .log-method.delete { background:rgba(239,68,68,.15); color:var(--error); }
  .log-code { display:inline-flex; align-items:center; justify-content:center; min-width:44px; height:26px; border-radius:4px; font-size:12px; font-weight:800; }
  .log-code.ok { background:rgba(72,187,120,.15); color:var(--green); }
  .log-code.warn { background:rgba(245,158,11,.15); color:var(--warn); }
  .log-code.err { background:rgba(239,68,68,.15); color:var(--error); }
  .log-ip { font-size:13px; color:var(--text2); font-family:var(--font-mono); flex-shrink:0; }
  .log-path { font-size:13px; color:var(--text); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex:1; min-width:0; cursor:pointer; }
  .log-path:hover { color:var(--accent); }
  .logs-empty { display:flex; flex-direction:column; align-items:center; justify-content:center; padding:40px 20px; color:var(--muted); text-align:center; }
  .logs-empty .icon { font-size:36px; opacity:.4; margin-bottom:10px; }

  /* Stremio table */
  .stremio-table { display:flex; flex-direction:column; gap:6px; }
  .stremio-row { display:grid; grid-template-columns: 80px 1fr 1fr auto; gap:10px; align-items:center;
    padding:10px 12px; background:var(--surface2); border:1px solid var(--border); border-radius:8px; }
  .stremio-row-head { font-size:10px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.5px;
    background:var(--surface3); border:1px solid var(--border); }
  .stremio-cell { display:flex; align-items:center; gap:8px; }
  .stremio-cell input { flex:1; background:var(--input-bg); border:1px solid var(--border); border-radius:6px;
    padding:8px 10px; color:var(--text); font-size:12px; font-family:var(--font-body); }
  .stremio-cell input:focus { outline:none; border-color:var(--accent); }
  .stremio-cell .stremio-del { padding:6px 10px; background:rgba(239,68,68,.1); color:var(--error); border:1px solid var(--error);
    border-radius:6px; cursor:pointer; font-size:12px; transition:all .15s; }
  .stremio-cell .stremio-del:hover { background:rgba(239,68,68,.2); }
  .stremio-empty { text-align:center; color:var(--muted); padding:20px; font-size:13px; }

  /* Stremio channel editor table */
  .stremio-ch-table { display:flex; flex-direction:column; gap:4px; }
  .stremio-ch-row { display:grid; grid-template-columns: 60px 1fr 100px 120px 160px 80px; gap:10px; align-items:center;
    padding:10px 12px; background:var(--surface2); border:1px solid var(--border); border-radius:8px;
    transition:background .15s; }
  .stremio-ch-row:hover { background:var(--card-hover); }
  .stremio-ch-head { font-size:10px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.5px;
    background:var(--surface3); border:1px solid var(--border); }
  .stremio-ch-row img { border-radius:4px; }
  .stremio-ch-row .btn-outline-sm { padding:4px 10px; font-size:11px; height:auto; }

  /* TV Channels table */
  .tv-ch-table { display:flex; flex-direction:column; gap:4px; }
  .tv-ch-row { display:grid; grid-template-columns: 50px 1fr 100px 90px 90px 80px; gap:10px; align-items:center;
    padding:10px 12px; background:var(--surface2); border:1px solid var(--border); border-radius:8px;
    transition:background .15s; cursor:pointer; }
  .tv-ch-row:hover { background:var(--card-hover); }
  .tv-ch-head { font-size:10px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.5px;
    background:var(--surface3); border:1px solid var(--border); cursor:default; }
  .tv-ch-row img { border-radius:4px; }
  .tv-epg.ok { color:var(--green); font-weight:700; }
  .tv-epg.warn { color:var(--warn); font-weight:700; }
  .tv-epg.missing { color:var(--muted); font-size:11px; }
  .tv-stream.ok { color:var(--accent); font-family:var(--font-mono); font-size:11px; }
  .tv-stream.missing { color:var(--muted); font-size:11px; }
  .tv-name.custom { color:var(--accent); font-weight:700; }
  .tv-name.auto { color:var(--text2); }

  /* TV Modal */
  .tv-modal { max-width:560px; }
  .form-row { margin-bottom:14px; }
  .form-row label { display:block; font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; margin-bottom:6px; }
  .form-row input { width:100%; background:var(--input-bg); border:1px solid var(--border); border-radius:8px; padding:10px 12px; color:var(--text); font-size:13px; }
  .form-row input:focus { outline:none; border-color:var(--accent); }
  .form-row input[readonly] { background:var(--surface2); color:var(--text2); }
  .form-actions { display:flex; gap:10px; justify-content:flex-end; margin-top:20px; padding-top:16px; border-top:1px solid var(--border); }

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
  .top-row { display:grid; grid-template-columns:30px minmax(0,1fr) 1.2fr 44px; align-items:center; gap:12px;
    padding:8px 0; border-bottom:1px solid var(--border); cursor:pointer; }
  .top-row:last-child { border-bottom:none; }
  .top-row:hover { background:var(--card-hover); }
  .top-logo { width:28px; height:28px; border-radius:6px; object-fit:cover; background:#000; flex-shrink:0; }
  .top-name { font-size:13px; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .top-bar { height:8px; background:var(--surface2); border-radius:6px; overflow:hidden; }
  .top-bar-fill { height:100%; background:var(--accent); border-radius:6px; opacity:.85; transition:width .8s cubic-bezier(.22,1,.36,1); }
  .top-plays { font-size:12px; color:var(--text2); text-align:right; font-family:var(--font-mono); font-weight:700; }
  .top-time { font-size:12px; color:var(--text2); font-weight:600; }

  /* État des services */
  .svc-row { display:flex; align-items:center; gap:12px; padding:9px 2px; border-bottom:1px solid var(--border); }
  .svc-row:last-child { border-bottom:none; }
  .svc-dot { width:9px; height:9px; border-radius:50%; flex-shrink:0; }
  .svc-dot.ok { background:var(--green); box-shadow:0 0 8px rgba(72,187,120,.6); }
  .svc-dot.stale { background:var(--warn); box-shadow:0 0 8px rgba(245,158,11,.5); }
  .svc-dot.ko { background:var(--error); box-shadow:0 0 8px rgba(239,68,68,.6); }
  .svc-name { font-size:13px; font-weight:600; color:var(--text); flex-shrink:0; }
  .svc-desc { font-size:12px; color:var(--muted); margin-left:auto; text-align:right; }
  .svc-count { font-family:var(--font-mono); font-weight:800; }
  .svc-count.ok { color:var(--green); }
  .svc-count.part { color:var(--warn); }
  .svc-count.ko { color:var(--error); }
  .svc-foot { border-top:1px solid var(--border); }

  /* En ce moment (lectures live) */
  .live-item { display:flex; align-items:center; gap:10px; padding:8px 2px; border-bottom:1px solid var(--border);
    text-decoration:none; color:var(--text); }
  .live-item:last-child { border-bottom:none; }
  .live-item:hover { background:var(--card-hover); }
  .live-dot { width:8px; height:8px; border-radius:50%; background:var(--green); box-shadow:0 0 8px rgba(72,187,120,.7);
    animation:pulse 1.6s infinite; flex-shrink:0; }
  .live-name { flex:1; font-size:13px; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .live-time { font-size:11px; color:var(--muted); font-family:var(--font-mono); flex-shrink:0; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }

  /* Programmes en cours (mini-EPG) */
  .now-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr)); gap:12px; }
  .now-card { display:flex; flex-direction:column; background:var(--surface2); border:1px solid var(--border);
    border-radius:12px; overflow:hidden; text-decoration:none; color:var(--text); transition:all .18s; }
  .now-card:hover { transform:translateY(-2px); border-color:var(--accent); box-shadow:0 10px 26px rgba(0,0,0,.35); }
  .now-tile { position:relative; aspect-ratio:16/9; background:var(--surface3); display:grid; place-items:center; overflow:hidden; }
  .now-logo { width:62%; max-height:62%; object-fit:contain; filter:drop-shadow(0 4px 12px rgba(0,0,0,.4)); }
  .now-live { position:absolute; top:8px; left:8px; display:flex; align-items:center; gap:5px; font-size:9px; font-weight:800;
    color:#fff; background:rgba(229,62,62,.92); padding:3px 9px; border-radius:999px; letter-spacing:.4px; }
  .now-live i { width:6px; height:6px; border-radius:50%; background:#fff; animation:pulse 1.4s infinite; }
  .now-body { padding:11px 13px 13px; display:flex; flex-direction:column; gap:5px; flex:1; }
  .now-ch { font-size:11px; font-weight:800; color:var(--accent); text-transform:uppercase; letter-spacing:.5px;
    overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .now-prog-title { font-size:13px; font-weight:700; color:var(--text); line-height:1.3;
    display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
  .now-prog-desc { font-size:11px; color:var(--muted); line-height:1.35;
    display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
  .now-bar { height:4px; border-radius:999px; background:var(--surface3); overflow:hidden; margin-top:2px; }
  .now-fill { height:100%; border-radius:999px; background:var(--accent); transition:width .4s; }
  .now-ends { font-size:10px; color:var(--muted); font-family:var(--font-mono); }
  .now-next { font-size:11px; color:var(--text2); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .now-next b { color:var(--accent); font-weight:700; }

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
          <button class="nav-item" data-page="programs" onclick="navigateTo('programs')">📺 Programmes</button>
          <button class="nav-item" data-page="sources" onclick="navigateTo('sources')">📡 Sources</button>
          <button class="nav-item" data-page="channels" onclick="navigateTo('channels')">📺 Chaînes TV</button>
          <button class="nav-item" data-page="logs" onclick="navigateTo('logs')">📋 Logs<span class="nav-badge" id="logs-badge" style="display:none">0</span></button>
          <button class="nav-item" data-page="settings" onclick="navigateTo('settings')">⚙️ Réglages</button>
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
              <div class="card-head"><div class="card-title">🩺 État des services</div><span class="card-desc" id="svc-total"></span></div>
              <div class="card-body" id="svc-services" style="padding-top:8px">
                <div class="fav-empty">chargement…</div>
              </div>
              <div class="card-head svc-foot" style="justify-content:flex-end">
                <button class="btn-outline-sm" id="svc-check-btn" onclick="checkServices()">↻ Vérifier maintenant</button>
              </div>
            </div>
          </div>

          <div class="ov-grid-2">
            <div class="card">
              <div class="card-head">
                <div class="card-title">Erreurs</div>
                <div class="chart-range">
                  <button class="range-btn active" data-range="60" onclick="setChartRange(60)">1h</button>
                  <button class="range-btn" data-range="1440" onclick="setChartRange(1440)">24h</button>
                  <button class="range-btn" data-range="10080" onclick="setChartRange(10080)">7j</button>
                </div>
                <span class="card-desc" id="err-total"></span>
              </div>
              <div class="card-body"><div class="ov-chart-wrap" id="err-chart"><div class="fav-empty">aucune donnée</div></div></div>
            </div>
            <div class="card">
              <div class="card-head"><div class="card-title">📡 En ce moment</div></div>
              <div class="card-body" id="live-list" style="padding-top:8px">
                <div class="fav-empty">aucune lecture en cours — ouvre une chaîne !</div>
              </div>
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
        </div>

        <!-- PAGE: PROGRAMMES -->
        <div class="page" id="page-programs">
          <div class="page-header">
            <div>
              <div class="page-title">Programmes</div>
              <div class="page-sub">Ce qui passe en ce moment sur les chaînes populaires</div>
            </div>
          </div>
          <div class="card">
            <div class="card-head" style="gap:12px">
              <div class="catalog-search">
                <span class="search-ico">🔍</span>
                <input type="search" id="now-q" placeholder="Rechercher une chaîne ou un programme…" oninput="renderNow()">
              </div>
              <span class="card-desc" id="now-total" style="white-space:nowrap"></span>
              <button class="btn-outline-sm" onclick="loadNow(true)" title="Rafraîchir les programmes">↻</button>
            </div>
            <div class="card-body" id="now-list" style="padding-top:8px">
              <div class="fav-empty">EPG pas encore chargé — va dans Réglages pour le rafraîchir</div>
            </div>
          </div>
        </div>

        <!-- PAGE: LOGS -->
        <div class="page" id="page-logs">
          <div class="page-header">
            <div>
              <div class="page-title">Logs</div>
              <div class="page-sub">Journal en direct des requêtes — 300 dernières entrées</div>
            </div>
          </div>

          <div class="card">
            <div class="card-head logs-toolbar">
              <div class="logs-stats" id="logs-stats">
                <span class="log-stat" id="stat-total"><b>0</b> total</span>
                <span class="log-stat ok" id="stat-2xx"><b>0</b> 2xx</span>
                <span class="log-stat warn" id="stat-4xx"><b>0</b> 4xx</span>
                <span class="log-stat err" id="stat-5xx"><b>0</b> 5xx</span>
              </div>
              <div class="logs-group logs-search">
                <input type="search" id="logs-search" placeholder="Filtrer chemin, IP, méthode…" oninput="renderLogs()">
                <span class="search-ico">🔍</span>
              </div>
              <div class="logs-group">
                <label>Méthode</label>
                <select class="logs-select" id="logs-method" onchange="renderLogs()">
                  <option value="">Toutes</option>
                  <option value="GET">GET</option>
                  <option value="POST">POST</option>
                  <option value="DELETE">DELETE</option>
                </select>
              </div>
              <div class="logs-group">
                <label>Auto-refresh</label>
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
            <div class="card-body" style="padding:0">
              <div class="logs-list" id="logs-list">
                <div class="logs-empty"><div class="icon">📋</div><p>En attente de logs…</p></div>
              </div>
            </div>
          </div>
        </div>

        <!-- PAGE: CHAÎNES TV -->
        <div class="page" id="page-channels">
          <div class="page-header">
            <div>
              <div class="page-title">Chaînes TV</div>
              <div class="page-sub">Gestion complète : nom, logo, flux, EPG</div>
            </div>
          </div>

          <div class="card">
            <div class="card-head">
              <div class="card-title">📺 Liste des chaînes</div>
              <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
                <div class="catalog-search" style="min-width:280px;flex:1">
                  <span class="search-ico">🔍</span>
                  <input type="search" id="tv-ch-search" placeholder="Filtrer (nom, ID, source, EPG)…" oninput="renderTvChannels()">
                </div>
                <span class="card-desc" id="tv-ch-count"></span>
                <button class="btn-outline-sm" onclick="loadTvChannels()">↻ Rafraîchir</button>
              </div>
            </div>
            <div class="card-body" style="padding-top:8px">
              <div class="tv-ch-table" id="tv-ch-list">
                <div class="tv-empty">Chargement…</div>
              </div>
            </div>
          </div>

          <!-- Modal édition chaîne -->
          <div class="modal" id="tv-ch-modal" style="display:none">
            <div class="modal-content tv-modal">
              <div class="modal-header">
                <h3 id="tv-modal-title">Éditer la chaîne</h3>
                <button class="modal-close" onclick="closeTvModal()">&times;</button>
              </div>
              <div class="modal-body">
                <input type="hidden" id="tv-modal-id">
                <input type="hidden" id="tv-modal-src">
                
                <div class="form-row">
                  <label>ID (lecture seule)</label>
                  <input type="text" id="tv-modal-id-input" readonly style="background:var(--surface2);color:var(--text2)">
                </div>
                <div class="form-row">
                  <label>Source</label>
                  <input type="text" id="tv-modal-src-input" readonly style="background:var(--surface2);color:var(--text2);text-transform:uppercase">
                </div>
                
                <div class="form-row">
                  <label>Nom de la chaîne</label>
                  <input type="text" id="tv-modal-name" placeholder="Nom personnalisé (vide = auto)">
                  <span id="tv-modal-orig-name" style="font-size:11px;color:var(--muted)"></span>
                </div>
                
                <div class="form-row">
                  <label>Logo (URL png/jpg)</label>
                  <input type="url" id="tv-modal-logo" placeholder="https://.../logo.png (vide = auto)">
                  <img id="tv-modal-logo-preview" src="" alt="" style="width:48px;height:27px;object-fit:cover;border-radius:4px;border:1px solid var(--border);margin-top:6px;display:none">
                  <span id="tv-modal-orig-logo" style="font-size:11px;color:var(--muted)"></span>
                </div>
                
                <div class="form-row">
                  <label>Flux (m3u8/mpd)</label>
                  <input type="url" id="tv-modal-stream" placeholder="https://.../stream.m3u8 (vide = auto)">
                  <span id="tv-modal-orig-stream" style="font-size:11px;color:var(--muted)"></span>
                </div>
                
                <div class="form-row">
                  <label>EPG (ID xmltv)</label>
                  <input type="text" id="tv-modal-epg" placeholder="ID xmltv pour l'EPG (vide = auto)">
                  <span id="tv-modal-orig-epg" style="font-size:11px;color:var(--muted)"></span>
                </div>
                
                <div class="form-actions">
                  <button class="btn-outline-sm" onclick="resetTvModal()">↺ Réinitialiser</button>
                  <button class="btn-outline-sm" onclick="deleteTvChannel()">🗑️ Supprimer override</button>
                  <button class="add-source-btn" onclick="saveTvChannel()">💾 Enregistrer</button>
                </div>
              </div>
            </div>
          </div>

        <!-- PAGE: REGLAGES -->
        <div class="page" id="page-settings">
          <div class="page-header">
            <div>
              <div class="page-title">Réglages</div>
              <div class="page-sub">Logos, EPG et catégories personnalisables</div>
            </div>
            <div class="header-actions">
              <button class="btn-outline-sm" onclick="loadSettings()">↻ Actualiser</button>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">🎨 Affichage</div></div>
            <div class="card-body">
              <label class="toggle-row">
                <div>
                  <div class="toggle-title">Logos réels des chaînes</div>
                  <div class="toggle-sub">Vraies logos des chaînes populaires, sinon posters générés</div>
                </div>
                <input type="checkbox" id="set-logos" onchange="saveSettings()">
              </label>
              <label class="toggle-row">
                <div>
                  <div class="toggle-title">EPG — programme en cours</div>
                  <div class="toggle-sub">Affiche « Maintenant / À suivre » sur les chaînes couvertes</div>
                </div>
                <input type="checkbox" id="set-epg" onchange="saveSettings()">
              </label>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">📡 Source EPG (XMLTV)</div></div>
            <div class="card-body">
              <input class="add-source-input" id="set-epg-url" type="url" placeholder="URL du fichier XMLTV (.xml ou .xml.gz)">
              <div style="display:flex;gap:10px;margin-top:12px;align-items:center;flex-wrap:wrap">
                <button class="add-source-btn" onclick="saveSettings()">💾 Enregistrer</button>
                <button class="btn-outline-sm" onclick="refreshEpg()">↻ Rafraîchir l'EPG</button>
                <span id="epg-status" style="color:var(--text2);font-size:13px">chargement…</span>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">🗂️ Catégories</div></div>
            <div class="card-body">
              <p style="color:var(--text2);font-size:13px;margin-bottom:14px">Attribuez une catégorie aux chaînes populaires. Elles alimentent le filtre « genre » de Stremio.</p>
              <div id="genre-edit" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px">chargement…</div>
            </div>
          </div>
        </div>

        <!-- PAGE: SOURCES -->
        <div class="page" id="page-sources">
          <div class="page-header">
            <div>
              <div class="page-title">Sources</div>
              <div class="page-sub">Ajoutez vos propres chaînes via le scraper dlstreams</div>
            </div>
          </div>

          <div class="card">
            <div class="card-head"><div class="card-title">📡 Ajouter une source</div></div>
            <div class="card-body">
              <div class="add-source-box">
                <input class="add-source-input" id="source-url" type="url" placeholder="Collez l'URL d'une page dlstreams (ex: https://dlstreams.st/watch.php?id=121)">
                <button class="add-source-btn" id="add-source-btn">🔍 Scraper & Ajouter</button>
                <div class="add-source-result" id="add-source-result"></div>
                <div class="add-source-preview" id="add-source-preview" style="display:none"></div>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-head">
              <div class="card-title">📋 Sources ajoutées manuellement</div>
              <span class="card-desc" id="manual-count" style="font-size:12px;color:var(--muted)"></span>
            </div>
            <div class="card-body">
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
            <div class="card-head" style="gap:12px">
              <div class="catalog-search">
                <span class="search-ico">🔍</span>
                <input type="search" id="q" placeholder="Rechercher une chaîne…">
              </div>
              <div class="tabs">
                <button class="tab active" data-src="dlstreams">dlstreams</button>
                <button class="tab" data-src="vavoo">Vavoo</button>
              </div>
            </div>
            <div class="catalog-subbar">
              <select id="lang-filter">
                <option value="all" selected>🌍 Toutes langues</option>
                <option value="fr">🇫🇷 Français</option>
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
              <span class="list-count" id="catalog-count"></span>
              <span style="flex:1"></span>
              <button class="btn-outline-sm" onclick="exportCatalogM3U()">⬇️ M3U</button>
            </div>
            <div class="card-body">
              <div class="channel-list" id="list"><div class="fav-empty">chargement…</div></div>
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
        settings: 'Logos, EPG et catégories personnalisables',
        programs: 'Programme en cours des chaînes populaires'
    };
    document.querySelector('.page.active .page-sub').textContent = subtitles[page] || '';

    if (page === 'sources') { loadManualChannels(); loadActivity("activity-list-src"); }
    if (page === 'logs') { loadLogs(); }
    if (page === 'settings') { loadSettings(); }
    if (page === 'channels') { loadTvChannels(); }
    if (page === 'programs') { loadNow(); }
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
        renderErrChart(d.hist_err || []);
        renderServices(d.health || {});
        LAST_TOP = d.top_channels || [];
        LAST_REC = d.recent_plays || [];
        renderChannelsCard();
        loadNow();
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
let LAST_HIST = [], LAST_HIST_ERR = [];
function setChartRange(mins){
    CHART_RANGE = mins;
    document.querySelectorAll('.range-btn').forEach(b=>b.classList.toggle('active', Number(b.dataset.range)===mins));
    renderAreaChart(LAST_HIST);
    renderErrChart(LAST_HIST_ERR);
}
function _areaChart(el, history, totalEl, accent, unit, gid){
    if(!el) return;
    if(!history || !history.length){ el.innerHTML = '<div class="fav-empty">aucune donnée</div>'; return; }
    const nowMin = Date.now() / 1000;
    const windowMin = CHART_RANGE === 10080 ? 10080 : (CHART_RANGE === 1440 ? 1440 : 60);
    const arr = history.filter(h => nowMin - h[0] <= windowMin + 1);
    if(totalEl) totalEl.textContent = arr.length ? arr.reduce((a,h)=>a+h[1], 0).toLocaleString('fr-FR') + ' ' + unit : '';
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
        return '<text x="'+p.x.toFixed(1)+'" y="'+(H-8)+'" text-anchor="middle" class="ov-chart-xlabel"'+(i===n-1?' style="fill:'+accent+';font-weight:800"':'')+'>'+
            fmt+'</text>';
    }).join('');
    const dots = pts.filter((_,i)=>i%Math.max(1,Math.round(n/40))===0).map(p => '<circle cx="'+p.x.toFixed(1)+'" cy="'+p.y.toFixed(1)+'" r="2.5" style="fill:var(--surface);stroke:'+accent+';stroke-width:2"><title>'+p.v+' '+unit+'</title></circle>').join('');
    el.innerHTML = '<svg class="ov-chart" viewBox="0 0 '+W+' '+H+'">' +
        '<defs><linearGradient id="'+gid+'" x1="0" y1="0" x2="0" y2="1">' +
        '<stop offset="0%" style="stop-color:'+accent+';stop-opacity:0.32"/>' +
        '<stop offset="100%" style="stop-color:'+accent+';stop-opacity:0"/>' +
        '</linearGradient></defs>' + grid +
        '<path d="'+area+'" fill="url(#'+gid+')"/>' +
        '<path d="'+line+'" fill="none" style="stroke:'+accent+'" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>' +
        '<circle cx="'+last.x.toFixed(1)+'" cy="'+last.y.toFixed(1)+'" r="9" fill="'+accent+'" opacity="0.18"/>' +
        '<circle cx="'+last.x.toFixed(1)+'" cy="'+last.y.toFixed(1)+'" r="4" fill="'+accent+'"/>' +
        dots + labels + '</svg>';
}
function renderAreaChart(history){
    LAST_HIST = history || [];
    _areaChart($("#traffic-chart"), LAST_HIST, $("#chart-total"), 'var(--accent)', 'requêtes', 'ov-fill');
}
function renderErrChart(history){
    LAST_HIST_ERR = history || [];
    _areaChart($("#err-chart"), LAST_HIST_ERR, $("#err-total"), 'var(--error)', 'erreurs', 'ov-fill-err');
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
        const logoUrl = t.src==="vavoo"
            ? `${BASE}/logo/vavoo/${encodeURIComponent(b64u(t.id))}.png`
            : `${BASE}/logo/dlstreams/${t.id}.png`;
        const logo = `<img class="top-logo" src="${logoUrl}" alt="" loading="lazy" onerror="this.style.display='none'">`;
        const bar = isTop ? Math.round((t.plays / max) * 100) : 0;
        return `<div class="top-row" data-play="${href}" title="${escapeHtml(t.name)}">
            ${logo}
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

// Carte "État des services"
function renderServices(h){
    const el = $("#svc-services");
    if(!el) return;
    if(!h || !h.at){ el.innerHTML = '<div class="fav-empty">vérification…</div>'; return; }
    const fmtMs = s => s && s.ms != null ? ` · ${s.ms} ms` : '';
    const rows = [
        ['dlstreams.st', h.dlstreams, h.dlstreams ? (h.dlstreams.ok ? 'OK' : 'KO') + fmtMs(h.dlstreams) : '…'],
        ['API Vavoo', h.vavoo, h.vavoo ? (h.vavoo.ok ? 'OK' : 'KO') + fmtMs(h.vavoo) : '…'],
        ['EPG', h.epg, h.epg ? (h.epg.ok ? `${h.epg.channels} chaînes` : h.epg.channels ? 'périmé' : 'non chargé')
            + (h.epg.age != null ? ' · ' + fmtAge(h.epg.age) : '') : '…'],
        ['Logos', h.logos, h.logos ? `${h.logos.loaded}/${h.logos.total} chargés` : '…'],
    ];
    el.innerHTML = rows.map(r => {
        const st = r[1] && r[1].ok ? 'ok' : 'ko';
        return `<div class="svc-row"><span class="svc-dot ${st}"></span><span class="svc-name">${r[0]}</span><span class="svc-desc">${r[2]}</span></div>`;
    }).join('');
    const okN = rows.filter(r => r[1] && r[1].ok).length;
    $("#svc-total").textContent = `${okN}/${rows.length} opérationnels`;
}
async function checkServices(){
    const btn = $("#svc-check-btn");
    if(btn){ btn.disabled = true; btn.textContent = "⏳ Vérification…"; }
    try{
        await apiFetch('/api/health', {method: 'POST'});
        await refreshStats();
        toast('🩺 Services vérifiés', 'success');
    }catch(e){
        if (e.message !== 'unauthenticated') toast('❌ Échec de la vérification', 'error');
    }
    if(btn){ btn.disabled = false; btn.textContent = "↻ Vérifier maintenant"; }
}

// En ce moment : lectures live (poll 5s)
async function loadLive(){
    try{
        const r = await apiFetch('/api/live');
        renderLive(await r.json());
    }catch(e){
        if (e.message !== 'unauthenticated') console.error("Live error:", e);
    }
}
function renderLive(list){
    const el = $("#live-list");
    if(!el) return;
    if(!list.length){
        el.innerHTML = '<div class="fav-empty">aucune lecture en cours — ouvre une chaîne !</div>';
        return;
    }
    el.innerHTML = list.map(t => {
        const href = t.src==="vavoo" ? `${BASE}/vhls?v=${encodeURIComponent(b64u(t.id))}` : `${BASE}/hls/${t.id}/index.m3u8`;
        return `<a class="live-item" href="${href}" target="_blank" title="${escapeHtml(t.name)}">
            <span class="live-dot"></span>
            <span class="live-name">${escapeHtml(t.name)}</span>
            <span class="live-time">${fmtAgo(t.last)}</span>
        </a>`;
    }).join('');
}

// Programmes en cours : mini-EPG des chaînes populaires
let _NOW_CACHE = [];
async function loadNow(force){
    try{
        if(force){
            const el = $("#now-list");
            if(el && !el.innerHTML.includes('chargement')) el.innerHTML = '<div class="fav-empty">chargement…</div>';
        }
        const r = await apiFetch('/api/now');
        _NOW_CACHE = await r.json();
        renderNow();
    }catch(e){
        if (e.message !== 'unauthenticated') console.error("Now error:", e);
    }
}
function renderNow(){
    const el = $("#now-list");
    if(!el) return;
    const tot = $("#now-total");
    const q = ($("#now-q").value || "").toLowerCase().trim();
    let list = _NOW_CACHE;
    if(!list || !list.length){
        el.innerHTML = '<div class="fav-empty">EPG pas encore chargé — va dans Réglages pour le rafraîchir</div>';
        if(tot) tot.textContent = '';
        return;
    }
    if(q) list = list.filter(n => (n.name||"").toLowerCase().includes(q)
        || ((n.cur&&n.cur.title)||"").toLowerCase().includes(q)
        || ((n.nxt&&n.nxt.title)||"").toLowerCase().includes(q));
    if(!list.length){
        el.innerHTML = '<div class="fav-empty">aucun résultat — essaie un autre filtre</div>';
        if(tot) tot.textContent = '0 chaîne';
        return;
    }
    if(tot) tot.textContent = list.length + (list.length>1 ? ' chaînes guidées' : ' chaîne guidée');
    el.innerHTML = '<div class="now-grid">' + list.map(n => {
        const href = `${BASE}/hls/${n.id}/index.m3u8`;
        const cur = n.cur || {};
        const nxt = n.nxt || {};
        const now = n.now || Math.floor(Date.now()/1000);
        const st = cur.start ? new Date(cur.start*1000).toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'}) : '';
        const en = cur.stop ? new Date(cur.stop*1000).toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'}) : '';
        let pct = 0, rem = 0;
        if(cur.start && cur.stop && cur.stop>cur.start){
            pct = Math.min(100, Math.max(0, Math.round((now-cur.start)/(cur.stop-cur.start)*100)));
            rem = Math.max(0, Math.round((cur.stop-now)/60));
        }
        const nxT = nxt.start ? new Date(nxt.start*1000).toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'}) : '';
        return `<a class="now-card" href="${href}" target="_blank" title="${escapeHtml(n.name)} — ${escapeHtml(cur.title||'')}">
            <div class="now-tile">
                <img class="now-logo" src="${BASE}${n.logo}" alt="" loading="lazy" onerror="this.style.display='none'">
                <span class="now-live"><i></i> EN DIRECT</span>
            </div>
            <div class="now-body">
                <div class="now-ch">${escapeHtml(n.name)}</div>
                <div class="now-prog-title">${escapeHtml(cur.title || '—')}</div>
                ${cur.desc ? `<div class="now-prog-desc">${escapeHtml(cur.desc)}</div>` : ''}
                <div class="now-bar"><div class="now-fill" style="width:${pct}%"></div></div>
                <div class="now-ends">${st}–${en} · ${rem} min restantes</div>
                ${nxt.title ? `<div class="now-next">Suivant · <b>${escapeHtml(nxt.title)}</b>${nxT ? ' ('+nxT+')' : ''}</div>` : ''}
            </div>
        </a>`;
    }).join('') + '</div>';
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

// Sources manuelles
async function loadManualChannels(){
    try{
        const r = await apiFetch("/api/manual-channels");
        const channels = await r.json();
        const list = $("#manual-channels-list");
        const cnt = $("#manual-count");
        if(cnt) cnt.textContent = channels.length + (channels.length>1 ? " sources" : " source");
        if(channels.length === 0){
            list.innerHTML = '<div style="color:var(--muted);text-align:center;padding:30px;grid-column:1/-1">Aucune source ajoutée</div>';
            return;
        }
        list.innerHTML = channels.map(ch => {
            const href = `${BASE}/hls/${ch.id}/index.m3u8`;
            const added = ch.added_at ? ' · ' + ch.added_at : '';
            return `<a class="manual-channel-card" href="${href}" target="_blank" title="${escapeHtml(ch.name)}">
                <div class="manual-tile">
                    <img class="manual-logo" src="${BASE}/logo/dlstreams/${ch.id}.png" alt="" loading="lazy" onerror="this.style.display='none'">
                    <span class="manual-badge">MANUEL</span>
                </div>
                <div class="manual-body">
                    <div class="manual-name">${escapeHtml(ch.name)}</div>
                    <div class="manual-meta">ID: ${escapeHtml(ch.id)}${added}</div>
                    <div class="manual-actions">
                        <button class="manual-test" onclick="event.preventDefault();event.stopPropagation();testManual('${escapeHtml(ch.id)}', this)">▶ Test</button>
                        <button class="manual-del" onclick="event.preventDefault();event.stopPropagation();removeChannel('${escapeHtml(ch.id)}')">🗑️ Supprimer</button>
                    </div>
                </div>
            </a>`;
        }).join('');
    }catch(e){
        if (e.message !== 'unauthenticated') console.error("Load manual channels error:", e);
    }
}
async function testManual(id, btn){
    if(CHECKED[id] && CHECKED[id].state==="busy") return;
    if(btn) btn.classList.add("busy");
    CHECKED[id] = {state:"busy"};
    render();
    try{
        const r = await apiFetch(`/api/check?src=dlstreams&id=${encodeURIComponent(id)}`);
        const d = await r.json();
        CHECKED[id] = {state: d.ok ? "ok" : "ko", ms: d.ms, url: d.url};
        saveChecked();
    }catch(e){
        if (e.message !== 'unauthenticated') CHECKED[id] = {state:"ko", ms:0};
    }
    if(btn) btn.classList.remove("busy");
    render();
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
let LANG_FILTER = localStorage.getItem("dl_lang") || "all";
const _LANG_FLAGS = {fr:"🇫🇷",en:"🇬🇧",es:"🇪🇸",de:"🇩🇪",it:"🇮🇹",ar:"🇸🇦",pt:"🇵🇹"};
function langFlag(l){ return _LANG_FLAGS[l] || "🌐"; }
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
    const url = src==="vavoo" ? "/api/vavoo-channels" : "/api/channels";
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
        list.innerHTML = '<div class="fav-empty">aucun résultat — essaie un autre filtre</div>';
        return;
    }
    list.innerHTML = items.map(c => {
        const encodedId = CURRENT==="vavoo" ? b64u(c.id) : c.id;
        const href = CURRENT==="vavoo"
            ? `${BASE}/vhls?v=${encodeURIComponent(encodedId)}`
            : `${BASE}/hls/${c.id}/index.m3u8`;
        const key = (CURRENT==="vavoo"?"vavoo:":"dlstreams:")+c.id;
        const logoUrl = CURRENT==="vavoo"
            ? `${BASE}/logo/vavoo/${encodeURIComponent(encodedId)}.png`
            : `${BASE}/logo/dlstreams/${c.id}.png`;
        const plays = PLAYS[key] || 0;
        const flag = langFlag(c.lang);
        const meta = plays
            ? `${flag} · ${plays} lecture${plays>1?'s':''}`
            : `${flag} · ${CURRENT==="vavoo" ? 'Vavoo' : '#' + c.id}`;
        return `<a class="chan-card" href="${href}" target="_blank" title="${escapeHtml(c.name)}" data-play="${href}">
            <div class="chan-tile">
                <img class="chan-logo" src="${logoUrl}" alt="" loading="lazy">
                <span class="chan-check ${checkCls(key)}" onclick="event.preventDefault();event.stopPropagation();checkStream('${escapeHtml(key)}')">${checkLabel(key)}</span>
                <span class="chan-play"><span class="play-badge">▶</span></span>
            </div>
            <div class="chan-body">
                <div class="chan-name">${escapeHtml(c.name)}</div>
                <div class="chan-meta">${escapeHtml(meta)}</div>
            </div>
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

// ===== REGLAGES =====
let _settings = null;

async function loadSettings(){
    try{
        const r = await apiFetch("/api/settings");
        const d = await r.json();
        _settings = d.settings;
        $('#set-logos').checked = !!_settings.logos;
        $('#set-epg').checked = !!_settings.epg;
        $('#set-epg-url').value = _settings.epg_url || '';
        const e = d.epg || {};
        $('#epg-status').textContent = e.at
            ? `Dernière MAJ ${new Date(e.at*1000).toLocaleTimeString('fr-FR')} · ${e.covered}/${e.channels} chaînes`
            : 'EPG pas encore chargé';
        renderGenreEditor(d);
    }catch(err){ if(err.message !== 'unauthenticated') toast('Erreur de chargement des réglages','error'); }
}

function renderGenreEditor(d){
    const box = $('#genre-edit');
    box.innerHTML = '';
    const choices = d.genre_choices || [];
    const overrides = (_settings && _settings.genres) || {};
    const popular = d.popular || [];
    if(!popular.length){ box.innerHTML = '<div class="fav-empty">aucune chaîne</div>'; return; }
    popular.forEach(c => {
        const key = c.name.toLowerCase();
        const cur = (overrides[key] && overrides[key][0]) || c.genre || '';
        const row = document.createElement('div');
        row.style.cssText = 'display:flex;align-items:center;gap:10px;justify-content:space-between;font-size:13px;min-width:0';
        row.innerHTML =
            `<span style="color:var(--text2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(c.name)}</span>` +
            `<select style="max-width:160px;background:var(--input-bg);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:5px 6px" ` +
            `onchange="setGenre('${key.replace(/[\\']/g, "\\'")}', this.value)">` +
            choices.map(g => `<option value="${escapeHtml(g)}" ${g===cur?'selected':''}>${escapeHtml(g)}</option>`).join('') +
            `</select>`;
        box.appendChild(row);
    });
}

async function setGenre(key, value){
    if(!_settings) return;
    const g = Object.assign({}, _settings.genres || {});
    g[key] = [value];
    _settings.genres = g;
    const r = await apiFetch("/api/settings", {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({genres:g})});
    if(r.ok) toast('Catégorie enregistrée','success'); else toast('Erreur d\'enregistrement','error');
}

async function saveSettings(){
    const body = {logos: $('#set-logos').checked, epg: $('#set-epg').checked, epg_url: $('#set-epg-url').value.trim()};
    const r = await apiFetch("/api/settings", {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    if(r.ok){ toast('Réglages enregistrés','success'); } else toast('Erreur d\'enregistrement','error');
}

// ===== STREMIO CHANNEL EDITOR =====
let _STREMIO_CHANNELS = [];
async function loadStremioChannels(){
    try{
        const r = await apiFetch("/api/settings");
        const d = await r.json();
        const s = d.settings.stremio || {};
        $('#stremio-manifest-name').value = s.manifest_name || '';
        $('#stremio-manifest-desc').value = s.manifest_desc || '';
        $('#stremio-default-lang').value = s.default_lang || 'fr';
        $('#stremio-inc-dl').checked = s.include_dlstreams !== false;
        $('#stremio-inc-vv').checked = s.include_vavoo !== false;
        updateStremioManifestUrl();

        // Build unified channel list with overrides
        const names = s.channel_names || {};
        const logos = s.channel_logos || {};
        const streams = s.channel_streams || {};

        _STREMIO_CHANNELS = [];
        [...(ALL.dlstreams||[]), ...(ALL.vavoo||[])].forEach(c => {
            const id = c.id;
            _STREMIO_CHANNELS.push({
                id,
                src: 'dlstreams',
                name: c.name,
                orig_name: c.name,
                override_name: names[id],
                override_logo: logos[id],
                override_stream: streams[id],
            });
        });
        [...(ALL.vavoo||[])].forEach(c => {
            // Avoid duplicates if same ID exists in both
            if (!_STREMIO_CHANNELS.some(x => x.id === c.id && x.src === 'vavoo')) {
                _STREMIO_CHANNELS.push({
                    id: c.id,
                    src: 'vavoo',
                    name: c.name,
                    orig_name: c.name,
                    override_name: names[c.id],
                    override_logo: logos[c.id],
                    override_stream: streams[c.id],
                });
            }
        });
        renderStremioChannels();
    }catch(err){ if(err.message !== 'unauthenticated') toast('Erreur chargement Stremio','error'); }
}

function updateStremioManifestUrl(){
    const base = window.location.origin;
    const lang = $('#stremio-default-lang').value;
    const url = lang === 'all' ? `${base}/manifest.json` : `${base}/manifest.json?lang=${lang}`;
    $('#stremio-manifest-url').textContent = url;
}

async function saveStremioSettings(){
    const s = {
        manifest_name: $('#stremio-manifest-name').value.trim(),
        manifest_desc: $('#stremio-manifest-desc').value.trim(),
        default_lang: $('#stremio-default-lang').value,
        include_dlstreams: $('#stremio-inc-dl').checked,
        include_vavoo: $('#stremio-inc-vv').checked,
    };
    const r = await apiFetch("/api/settings", {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({stremio:s})});
    if(r.ok){
        toast('Manifest Stremio enregistré','success');
        updateStremioManifestUrl();
    }else toast('Erreur enregistrement','error');
}

function renderStremioChannels(){
    const q = ($('#stremio-ch-search').value || '').toLowerCase().trim();
    const list = $('#stremio-ch-list');
    const cnt = $('#stremio-ch-count');
    let channels = _STREMIO_CHANNELS;
    if(q) channels = channels.filter(c =>
        (c.id||'').toLowerCase().includes(q) ||
        (c.name||'').toLowerCase().includes(q) ||
        (c.src||'').toLowerCase().includes(q)
    );
    if(cnt) cnt.textContent = channels.length + (channels.length>1?' chaînes':' chaîne');
    if(!channels.length){
        list.innerHTML = '<div class="stremio-empty">Aucune chaîne</div>';
        return;
    }
    list.innerHTML = '<div class="stremio-ch-row stremio-ch-head"><div style="width:60px">ID</div><div>Nom</div><div style="width:100px">Source</div><div style="width:120px">Logo</div><div style="width:160px">Flux</div><div style="width:80px"></div></div>' +
        channels.map(c => {
            const hasName = !!c.override_name;
            const hasLogo = !!c.override_logo;
            const hasStream = !!c.override_stream;
            const logoPreview = c.override_logo ? `<img src="${escapeHtml(c.override_logo)}" style="width:32px;height:18px;object-fit:cover;border-radius:3px;border:1px solid var(--border)" onerror="this.style.display='none'">` : `<span style="color:var(--muted);font-size:11px">Auto</span>`;
            const streamPreview = c.override_stream ? `<span style="color:var(--accent);font-family:var(--font-mono);font-size:11px">${escapeHtml(c.override_stream).substring(0,50)}…</span>` : `<span style="color:var(--muted);font-size:11px">Auto</span>`;
            const nameDisplay = c.override_name ? `<span style="color:var(--accent)">${escapeHtml(c.override_name)}</span>` : `<span style="color:var(--text2)">${escapeHtml(c.orig_name)}</span>`;
            return `<div class="stremio-ch-row" data-id="${escapeHtml(c.id)}" data-src="${escapeHtml(c.src)}">
                <div style="width:60px;font-family:var(--font-mono);font-size:11px;color:var(--text2)">${escapeHtml(c.id)}</div>
                <div>${nameDisplay}</div>
                <div style="width:100px;text-transform:uppercase;font-size:11px;color:var(--muted)">${c.src}</div>
                <div style="width:120px">${logoPreview}</div>
                <div style="width:160px">${streamPreview}</div>
                <div style="width:80px"><button class="btn-outline-sm" onclick="editStremioChannel('${escapeHtml(c.id)}','${escapeHtml(c.src)}')" style="padding:4px 10px;font-size:11px">✏️</button></div>
            </div>`;
        }).join('');
}

function editStremioChannel(id, src){
    const c = _STREMIO_CHANNELS.find(x => x.id === id && x.src === src);
    if(!c) return;
    document.getElementById('stremio-channel-form').style.display = 'block';
    $('#stremio-ch-id').value = c.id;
    $('#stremio-ch-src').value = c.src;
    $('#stremio-ch-name').value = c.override_name || '';
    $('#stremio-ch-orig-name').textContent = c.orig_name ? `Original: ${c.orig_name}` : '';
    $('#stremio-ch-logo').value = c.override_logo || '';
    $('#stremio-ch-logo-preview').src = c.override_logo || '';
    $('#stremio-ch-logo-preview').style.display = c.override_logo ? 'inline-block' : 'none';
    $('#stremio-ch-orig-logo').textContent = c.override_logo ? '' : `Auto: /logo/${c.src}/${c.id}.png`;
    $('#stremio-ch-stream').value = c.override_stream || '';
    $('#stremio-ch-orig-stream').style.display = c.override_stream ? 'none' : 'inline';
    window._editingStremio = {id: c.id, src: c.src};
    $('#stremio-ch-name').focus();
}

function resetStremioChannel(){
    if(!window._editingStremio) return;
    const c = _STREMIO_CHANNELS.find(x => x.id === window._editingStremio.id && x.src === window._editingStremio.src);
    if(!c) return;
    $('#stremio-ch-name').value = c.override_name || '';
    $('#stremio-ch-logo').value = c.override_logo || '';
    $('#stremio-ch-logo-preview').src = c.override_logo || '';
    $('#stremio-ch-logo-preview').style.display = c.override_logo ? 'inline-block' : 'none';
    $('#stremio-ch-stream').value = c.override_stream || '';
    $('#stremio-ch-name').focus();
}

async function saveStremioChannel(){
    const e = window._editingStremio;
    if(!e) return;
    const name = $('#stremio-ch-name').value.trim();
    const logo = $('#stremio-ch-logo').value.trim();
    const stream = $('#stremio-ch-stream').value.trim();

    const r = await apiFetch("/api/settings", {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
        stremio: {
            channel_names: { ...(_settings.stremio?.channel_names||{}), [e.id]: name || undefined },
            channel_logos: { ...(_settings.stremio?.channel_logos||{}), [e.id]: logo || undefined },
            channel_streams: { ...(_settings.stremio?.channel_streams||{}), [e.id]: stream || undefined },
        }
    })});
    if(r.ok){
        toast('Chaîne enregistrée','success');
        await loadStremioChannels();
        cancelStremioChannel();
    }else toast('Erreur','error');
}

async function deleteStremioChannel(){
    if(!window._editingStremio) return;
    if(!confirm('Supprimer l\'override de cette chaîne ?')) return;
    const e = window._editingStremio;
    const r = await apiFetch("/api/settings", {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
        stremio: {
            channel_names: Object.fromEntries(Object.entries(_settings.stremio?.channel_names||{}).filter(([k])=>k!==e.id)),
            channel_logos: Object.fromEntries(Object.entries(_settings.stremio?.channel_logos||{}).filter(([k])=>k!==e.id)),
            channel_streams: Object.fromEntries(Object.entries(_settings.stremio?.channel_streams||{}).filter(([k])=>k!==e.id)),
        }
    })});
    if(r.ok){
        toast('Override supprimé','success');
        await loadStremioChannels();
        cancelStremioChannel();
    }else toast('Erreur','error');
}

function cancelStremioChannel(){
    document.getElementById('stremio-channel-form').style.display = 'none';
    window._editingStremio = null;
}

function testStremioChannelStream(){
    const url = $('#stremio-ch-stream').value.trim();
    if(url) window.open(url, '_blank');
}

async function saveStremioObj(patch){
    const s = { ...(_settings.stremio||{}), ...patch };
    const r = await apiFetch("/api/settings", {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({stremio:s})});
    if(r.ok){
        _settings.stremio = s;
        toast('Enregistré','success');
    }else toast('Erreur','error');
}

async function saveTvChannel(){
    const id = $('#tv-modal-id').value;
    const src = $('#tv-modal-src').value;
    if(!id || !src) return;

    const name = $('#tv-modal-name').value.trim();
    const logo = $('#tv-modal-logo').value.trim();
    const stream = $('#tv-modal-stream').value.trim();
    const epg = $('#tv-modal-epg').value.trim();

    const patch = {
        channel_names: { ...(_settings.stremio?.channel_names||{}), [id]: name || undefined },
        channel_logos: { ...(_settings.stremio?.channel_logos||{}), [id]: logo || undefined },
        channel_streams: { ...(_settings.stremio?.channel_streams||{}), [id]: stream || undefined },
        channel_epg: { ...(_settings.stremio?.channel_epg||{}), [id]: epg || undefined },
    };

    const r = await apiFetch("/api/settings", {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({stremio:patch})});
    if(r.ok){
        toast('Chaîne enregistrée','success');
        closeTvModal();
        await loadTvChannels();
    }else toast('Erreur','error');
}

async function deleteTvChannel(){
    const id = $('#tv-modal-id').value;
    const src = $('#tv-modal-src').value;
    if(!id || !src) return;
    if(!confirm('Supprimer l\'override de cette chaîne ?')) return;

    const patch = {
        channel_names: Object.fromEntries(Object.entries(_settings.stremio?.channel_names||{}).filter(([k])=>k!==id)),
        channel_logos: Object.fromEntries(Object.entries(_settings.stremio?.channel_logos||{}).filter(([k])=>k!==id)),
        channel_streams: Object.fromEntries(Object.entries(_settings.stremio?.channel_streams||{}).filter(([k])=>k!==id)),
        channel_epg: Object.fromEntries(Object.entries(_settings.stremio?.channel_epg||{}).filter(([k])=>k!==id)),
    };

    const r = await apiFetch("/api/settings", {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({stremio:patch})});
    if(r.ok){
        toast('Override supprimé','success');
        closeTvModal();
        loadTvChannels();
    }else toast('Erreur','error');
}

function resetTvModal(){
    if(!window._editingTvChannel) return;
    const c = _TV_CHANNELS.find(x => x.id === window._editingTvChannel.id && x.src === window._editingTvChannel.src);
    if(!c) return;
    $('#tv-modal-name').value = c.override_name || '';
    $('#tv-modal-logo').value = c.override_logo || '';
    $('#tv-modal-logo-preview').src = c.override_logo || '';
    $('#tv-modal-logo-preview').style.display = c.override_logo ? 'block' : 'none';
    $('#tv-modal-stream').value = c.override_stream || '';
    $('#tv-modal-epg').value = c.override_epg || '';
    $('#tv-modal-name').focus();
}

function closeTvModal(){
    document.getElementById('tv-ch-modal').style.display = 'none';
    window._editingTvChannel = null;
}

function openTvModal(id, src){
    const c = _TV_CHANNELS.find(x => x.id === id && x.src === src);
    if(!c) return;
    window._editingTvChannel = {id: c.id, src: c.src};

    $('#tv-modal-title').textContent = `Éditer : ${c.name}`;
    $('#tv-modal-id').value = c.id;
    $('#tv-modal-src').value = c.src;
    $('#tv-modal-id-input').value = c.id;
    $('#tv-modal-src-input').value = c.src;
    $('#tv-modal-name').value = c.override_name || '';
    $('#tv-modal-orig-name').textContent = c.override_name ? `Original: ${c.name}` : '';
    $('#tv-modal-logo').value = c.override_logo || '';
    $('#tv-modal-logo-preview').src = c.override_logo || '';
    $('#tv-modal-logo-preview').style.display = c.override_logo ? 'block' : 'none';
    $('#tv-modal-orig-logo').textContent = c.override_logo ? '' : `Auto: /logo/${c.src}/${c.id}.png`;
    $('#tv-modal-stream').value = c.override_stream || '';
    $('#tv-modal-orig-stream').textContent = c.override_stream ? '' : `Auto: /hls/${c.id}/index.m3u8`;
    $('#tv-modal-epg').value = c.override_epg || '';
    $('#tv-modal-orig-epg').textContent = c.override_epg ? '' : `Auto: ${window._EPG_MAP[c.id] || '—'}`;

    document.getElementById('tv-ch-modal').style.display = 'flex';
    $('#tv-modal-name').focus();
}

let _TV_CHANNELS = [];
async function loadTvChannels(){
    try{
        // Ensure catalog is loaded
        if(!ALL.dlstreams.length) await loadCatalog('dlstreams');
        if(!ALL.vavoo.length) await loadCatalog('vavoo');

        const r = await apiFetch("/api/settings");
        if(!r.ok) throw new Error('Settings API failed: ' + r.status);
        const d = await r.json();
        window._EPG_MAP = d.epg_map || {};
        const s = d.settings.stremio || {};

        _TV_CHANNELS = [];
        [...(ALL.dlstreams||[]), ...(ALL.vavoo||[])].forEach(c => {
            const id = c.id;
            _TV_CHANNELS.push({
                id,
                src: 'dlstreams',
                name: c.name,
                orig_name: c.name,
                override_name: s.channel_names?.[id],
                override_logo: s.channel_logos?.[id],
                override_stream: s.channel_streams?.[id],
                override_epg: s.channel_epg?.[id],
                epg_id: window._EPG_MAP[c.id],
            });
        });
        [...(ALL.vavoo||[])].forEach(c => {
            if (!_TV_CHANNELS.some(x => x.id === c.id && x.src === 'vavoo')) {
                _TV_CHANNELS.push({
                    id: c.id,
                    src: 'vavoo',
                    name: c.name,
                    orig_name: c.name,
                    override_name: s.channel_names?.[c.id],
                    override_logo: s.channel_logos?.[c.id],
                    override_stream: s.channel_streams?.[c.id],
                    override_epg: s.channel_epg?.[c.id],
                    epg_id: window._EPG_MAP[c.id],
                });
            }
        });
        renderTvChannels();
    }catch(err){ if(err.message !== 'unauthenticated') toast('Erreur chargement chaînes: ' + err.message,'error'); }
}

function renderTvChannels(){
    const q = ($('#tv-ch-search').value || '').toLowerCase().trim();
    const list = $('#tv-ch-list');
    const cnt = $('#tv-ch-count');
    let channels = _TV_CHANNELS;
    if(q) channels = channels.filter(c =>
        (c.id||'').toLowerCase().includes(q) ||
        (c.name||'').toLowerCase().includes(q) ||
        (c.src||'').toLowerCase().includes(q) ||
        (c.epg_id||'').toLowerCase().includes(q)
    );
    if(cnt) cnt.textContent = channels.length + (channels.length>1?' chaînes':' chaîne');
    if(!channels.length){
        list.innerHTML = '<div class="tv-empty">Aucune chaîne</div>';
        return;
    }
    list.innerHTML = '<div class="tv-ch-row tv-ch-head"><div style="width:50px">Logo</div><div>Nom</div><div style="width:100px">Source</div><div style="width:90px">EPG</div><div style="width:90px">Flux</div><div style="width:80px"></div></div>' +
        channels.map(c => {
            const hasName = !!c.override_name;
            const hasLogo = !!c.override_logo;
            const hasStream = !!c.override_stream;
            const hasEpg = !!c.override_epg;
            const logoHtml = c.override_logo ? `<img src="${escapeHtml(c.override_logo)}" style="width:36px;height:20px;object-fit:cover;border-radius:3px;border:1px solid var(--border)">` : `<span style="color:var(--muted);font-size:11px">Auto</span>`;
            const streamHtml = c.override_stream ? `<span class="tv-stream ok">${escapeHtml(c.override_stream).substring(0,35)}…</span>` : `<span class="tv-stream missing">Auto</span>`;
            const epgHtml = c.override_epg ? `<span class="tv-epg ok">${escapeHtml(c.override_epg)}</span>` : (c.epg_id ? `<span class="tv-epg warn">${escapeHtml(c.epg_id)}</span>` : `<span class="tv-epg missing">—</span>`);
            const nameHtml = c.override_name ? `<span class="tv-name custom">${escapeHtml(c.override_name)}</span>` : `<span class="tv-name auto">${escapeHtml(c.name)}</span>`;
            return `<div class="tv-ch-row" data-id="${escapeHtml(c.id)}" data-src="${escapeHtml(c.src)}" onclick="openTvModal('${escapeHtml(c.id)}','${escapeHtml(c.src)}')">
                <div style="width:50px">${logoHtml}</div>
                <div>${nameHtml}</div>
                <div style="width:100px;text-transform:uppercase;font-size:11px;color:var(--muted)">${c.src}</div>
                <div style="width:90px;text-align:center">${epgHtml}</div>
                <div style="width:90px;text-align:center">${streamHtml}</div>
                <div style="width:80px;text-align:center"><span style="color:var(--muted);font-size:11px">✏️</span></div>
            </div>`;
        }).join('');
}

function testStremioStream(url){
    window.open(url, '_blank');
}

async function refreshEpg(){
    const st = $('#epg-status');
    st.textContent = 'Rafraîchissement en cours…';
    try{
        const r = await apiFetch("/api/epg/refresh", {method:'POST'});
        if(r.ok){ toast('Rafraîchissement EPG lancé','success'); setTimeout(loadSettings, 4000); }
        else toast('Erreur','error');
    }catch(err){ if(err.message !== 'unauthenticated') toast('Erreur','error'); }
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
    const card = e.target.closest(".chan-card, .channel-item");
    if(card && card.dataset.play){
        e.preventDefault();
        const name = (card.querySelector(".chan-name") || card.querySelector(".name")).textContent;
        openPlayer(card.dataset.play, name);
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
    const preview = $("#add-source-preview");
    if(!url){
        out.innerHTML = '<div class="alert alert-error">Veuillez entrer une URL</div>';
        return;
    }
    $("#add-source-btn").disabled = true;
    $("#add-source-btn").textContent = "⏳ Scraping...";
    out.innerHTML = "";
    preview.style.display = "none";
    preview.innerHTML = "";
    try{
        const r = await apiFetch(`/api/add-source?url=${encodeURIComponent(url)}`);
        const d = await r.json();
        if(d.success){
            out.innerHTML = `<div class="alert alert-success">${d.message}</div>`;
            if(d.channels && d.channels.length){
                preview.style.display = "block";
                preview.innerHTML = d.channels.map(ch => `
                    <div class="add-source-preview-item">
                        <div class="add-source-preview-name">${escapeHtml(ch.name)}</div>
                        <div class="add-source-preview-id">ID: ${escapeHtml(ch.id)}</div>
                    </div>`).join('');
            }
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
$("#lang-filter").addEventListener("change", (e) => {
    LANG_FILTER = e.target.value;
    localStorage.setItem("dl_lang", e.target.value);
    render();
});
if($("#lang-filter")) $("#lang-filter").value = LANG_FILTER;
if($("#catalog-sort")) $("#catalog-sort").value = SORT;
document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",async ()=>{
    document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));
    b.classList.add("active");
    CURRENT = b.dataset.src;
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

function updateLogsStats() {
    const total = allLogs.length;
    const c2 = allLogs.filter(l => l.code >= 200 && l.code < 300).length;
    const c4 = allLogs.filter(l => l.code >= 400 && l.code < 500).length;
    const c5 = allLogs.filter(l => l.code >= 500).length;
    const eTotal = document.getElementById('stat-total');
    const e2xx = document.getElementById('stat-2xx');
    const e4xx = document.getElementById('stat-4xx');
    const e5xx = document.getElementById('stat-5xx');
    if (eTotal) eTotal.querySelector('b').textContent = total;
    if (e2xx) e2xx.querySelector('b').textContent = c2;
    if (e4xx) e4xx.querySelector('b').textContent = c4;
    if (e5xx) e5xx.querySelector('b').textContent = c5;
}

function renderLogs() {
    const methodFilter = document.getElementById('logs-method').value;
    const search = document.getElementById('logs-search').value.trim().toLowerCase();

    let filtered = [...allLogs].reverse();
    if (methodFilter) filtered = filtered.filter(l => l.method === methodFilter);
    if (search) filtered = filtered.filter(l =>
        String(l.path||'').toLowerCase().includes(search)
        || String(l.ip||'').toLowerCase().includes(search)
        || String(l.method||'').toLowerCase().includes(search));

    updateLogsStats();

    const list = document.getElementById('logs-list');
    if (!filtered.length) {
        list.innerHTML = '<div class="logs-empty"><div class="icon">📋</div><p>Aucun log ne correspond</p></div>';
        return;
    }
    const wasAtBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 40;
    list.innerHTML = filtered.slice(0, 300).map(l => {
        const warn = l.code >= 400 && l.code < 500;
        const err = l.code >= 500;
        const methodCls = String(l.method||'').toLowerCase();
        const codeCls = l.code >= 500 ? 'err' : (l.code >= 400 ? 'warn' : 'ok');
        return `<div class="log-entry ${warn?'warn':''} ${err?'err':''}">
            <span class="log-time">${escapeHtml(l.t)}</span>
            <span class="log-method ${methodCls}">${escapeHtml(l.method)}</span>
            <span class="log-code ${codeCls}">${l.code}</span>
            <span class="log-ip">${escapeHtml(l.ip)}</span>
            <span class="log-path" title="${escapeHtml(l.path)}">${escapeHtml(l.path)}</span>
        </div>`;
    }).join('');
    if (wasAtBottom) list.scrollTop = list.scrollHeight;
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
async function boot(){
    await Promise.all([refreshStats(), loadCatalog("dlstreams"), loadPlays()]);
    render();
    loadLogs();
    loadLive();
    restartLogPolling();
    setInterval(refreshStats, 30000);
    setInterval(loadLive, 5000);
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
