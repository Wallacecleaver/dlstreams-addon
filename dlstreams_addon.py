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
3. expose un ADDON STREMIO minimal (manifest + catalog + stream) par-dessus.
Lancer :    python3 dlstreams_addon.py            (port 8781 par defaut, override: PORT=... )
VLC/ffmpeg: http://127.0.0.1:8781/hls/121/index.m3u8
Stremio :   installer via  http://<ton-ip-LAN>:8781/manifest.json
Rien d'autre a installer : Python 3.8+ suffit. Aucune cle, aucun compte.
"""
from __future__ import annotations
import base64
import json
import os
import re
import time
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8781"))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
SITE = "https://dlstreams.st"                 # wrapper : sa home = l'annuaire, ses pages = le pointeur DaddyLive
_CH_TTL = 1800                                # cache annuaire 30 min

# ============================================================================
# FILTRE CHAINES FRANCAISES - IDs DaddyLive (TOUS les IDs francais)
# ============================================================================
FRENCH_CHANNEL_IDS = {
    # TNT / Generales
    "469",   # TF1 France
    "950",   # France 2
    "951",   # France 3
    "952",   # France 4
    "953",   # France 5
    "470",   # M6 France
    "956",   # C8 France
    "957",   # BFM TV France
    "964",   # CNews France
    "955",   # TMC France
    "963",   # 6ter France
    "959",   # W9 France
    "958",   # Arte France
    "954",   # RMC Story France
    "962",   # LCI France
    "645",   # L'Equipe France
    
    # Canal+
    "121",   # Canal+ France
    "122",   # Canal+ Sport France
    "463",   # Canal+ Foot France
    "464",   # Canal+ Sport360
    "271",   # Canal+ MotoGP France
    "273",   # Canal+ Formula 1
    
    # beIN Sports
    "116",   # beIN SPORTS 1 France
    "117",   # beIN SPORTS 2 France
    "118",   # beIN SPORTS 3 France
    "494",   # beIN Sports MAX 4 France
    "495",   # beIN Sports MAX 5 France
    "496",   # beIN Sports MAX 6 France
    "497",   # beIN Sports MAX 7 France
    "498",   # beIN Sports MAX 8 France
    "499",   # beIN Sports MAX 9 France
    "500",   # beIN Sports MAX 10 France
    
    # RMC Sport
    "119",   # RMC Sport 1 France
    "120",   # RMC Sport 2 France
    
    # Eurosport
    "772",   # Eurosport 1 France
    "773",   # Eurosport 2 France
    
    # DAZN
    "960",   # DAZN Ligue 1 France
    
    # Sport
    "965",   # Sport en France
}

def _get(url: str, referer: str = SITE + "/", extra: dict | None = None, timeout: int = 20) -> bytes:
    """GET brut avec User-Agent + Referer (et Origin si fourni). Renvoie le corps (bytes)."""
    headers = {"User-Agent": UA, "Referer": referer}
    if extra:
        headers.update(extra)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:      # noqa: S310 (URL de conf, pas d'entree user)
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
    """Annuaire dlstreams -> [{id, name}]. Cache 30 min. Filtre chaines francaises uniquement."""
    now = time.time()
    if _ch_cache["list"] and now - _ch_cache["at"] < _CH_TTL:
        return _ch_cache["list"]
    try:
        html = _get(SITE + "/").decode("utf-8", "replace")
    except Exception:
        return _ch_cache["list"]                                 # garde l'ancien annuaire si la home tombe
    p = _LinkParser()
    p.feed(html)
    seen: dict[str, str] = {}
    for idv, name in p.items:
        seen.setdefault(idv, name)
    
    # Filtre pour garder uniquement les chaines francaises
    all_channels = [{"id": i, "name": n} for i, n in seen.items()]
    french_channels = [c for c in all_channels if c["id"] in FRENCH_CHANNEL_IDS]
    
    _ch_cache.update(at=now, list=french_channels)
    return french_channels

def search(query: str, limit: int = 40) -> list[dict]:
    q = query.lower().strip()
    hits = [c for c in channels() if q in c["name"].lower()]
    return hits[:limit]

# ---------------------------------------------------------------- resolution du flux (MULTI-PLAYER)
# dlstreams sert la MEME chaine sous plusieurs chemins de player, chacun pointant un embed/CDN DIFFERENT
# (daddy2/romponalis, wideiptv/bluetier, brigittetv, livelive24...). On les propose TOUS + failover.
_PLAYER_PATHS = ("stream", "watch", "player", "plus", "hub", "cast", "casting")

def _txt(url: str, referer: str = SITE + "/") -> str:
    return _get(url, referer=referer).decode("utf-8", "replace")

def players(cid: str) -> list[tuple[str, str]]:
    """[(label, url_page_player)] dans l'ordre de watch.php (repli = chemins connus). Chaque player est
    une source independante."""
    try:
        w = _txt(f"{SITE}/watch.php?id={cid}")
    except Exception:  # best-effort
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

# ---------------------------------------------------------------- proxy HLS (headers + fix content-type)
def _b64u(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")

def _unb64u(s: str) -> str:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()

def _rewrite_playlist(text: str, playlist_url: str, host: str, self_base: str) -> str:
    """Reecrit une playlist HLS : chaque URL (sous-playlist .m3u8 ou segment) passe par NOTRE proxy.
    Les sous-playlists gardent le host embed (fetch avec headers) ; les segments non (R2 auto-signe)."""
    base = playlist_url.rsplit("/", 1)[0] + "/"
    out = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            out.append(line)
            continue
        absu = s if s.startswith("http") else urllib.parse.urljoin(base, s)
        if ".m3u8" in absu.split("?", 1)[0]:
            out.append(f"{self_base}/px?u={_b64u(absu)}&h={_b64u(host)}")
        else:
            out.append(f"{self_base}/sx?u={_b64u(absu)}")
    return "\n".join(out)

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):                                   # silencieux
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
        # Derriere un proxy TLS (Render, Railway, Cloudflare...) le scheme public est https :
        # on suit X-Forwarded-Proto, sinon http en local. Sans ca, les URLs de flux sortent en
        # http:// sur un host https -> Stremio les bloque.
        host = self.headers.get("Host") or f"127.0.0.1:{PORT}"
        proto = self.headers.get("X-Forwarded-Proto", "http").split(",")[0].strip()
        return f"{proto}://{host}"

    def do_OPTIONS(self):
        # Preflight CORS de Stremio Web (app.strem.io) : sans reponse valide, l'install du manifest
        # echoue. On repond 204 + en-tetes CORS complets.
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):                                            # noqa: C901 (routeur plat, lisible)
        u = urllib.parse.urlsplit(self.path)
        path = u.path
        qs = urllib.parse.parse_qs(u.query)
        try:
            # ---- addon Stremio ----
            if path in ("/", "/manifest.json"):
                return self._send(200, json.dumps(self._manifest()).encode(), "application/json", True)
            if path.startswith("/catalog/tv/"):
                metas = [self._meta(c) for c in channels()]
                return self._send(200, json.dumps({"metas": metas}).encode(), "application/json", True)
            if path.startswith("/meta/tv/"):
                cid = path.rsplit("/", 1)[1].removesuffix(".json").split(":")[-1]
                c = next((x for x in channels() if x["id"] == cid), {"id": cid, "name": f"dlstreams {cid}"})
                return self._send(200, json.dumps({"meta": self._meta(c)}).encode(), "application/json", True)
            if path.startswith("/stream/tv/"):
                cid = path.rsplit("/", 1)[1].removesuffix(".json").split(":")[-1]
                b = self._self_base()
                # Auto (failover) en tête, puis TOUS les players (chacun un CDN distinct) au choix.
                streams = [{"name": "dlstreams", "title": "🔀 Auto (failover)",
                            "url": f"{b}/hls/{cid}/index.m3u8"}]
                for i, (label, _url) in enumerate(players(cid)):
                    streams.append({"name": "dlstreams", "title": label,
                                    "url": f"{b}/hls/{cid}/p{i}/index.m3u8"})
                return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")

            # ---- proxy HLS ---- (/hls/<id>/index.m3u8 = failover ; /hls/<id>/p<N>/index.m3u8 = player N)
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
                text = _get(m3u8, referer=host + "/", extra={"Origin": host}).decode("utf-8", "replace")
                body = _rewrite_playlist(text, m3u8, host, self._self_base()).encode()
                return self._send(200, body, "application/vnd.apple.mpegurl")

            if path == "/px":                                    # sous-playlist -> fetch avec headers + reecrit
                url = _unb64u(qs["u"][0])
                host = _unb64u(qs["h"][0])
                text = _get(url, referer=host + "/", extra={"Origin": host}).decode("utf-8", "replace")
                body = _rewrite_playlist(text, url, host, self._self_base()).encode()
                return self._send(200, body, "application/vnd.apple.mpegurl")

            if path == "/sx":                                    # segment -> fetch SANS header + rebalise en TS
                url = _unb64u(qs["u"][0])
                data = _get(url, timeout=25)
                return self._send(200, data, "video/mp2t")

            return self._send(404, b"not found", "text/plain")

        except Exception as e:                                   # noqa: BLE001 -- un flux mort ne tue pas le serveur
            return self._send(502, f"resolve/proxy error: {type(e).__name__}: {e}".encode(), "text/plain")

    # ---- helpers addon ----
    def _manifest(self) -> dict:
        return {
            "id": "st.dlstreams.proxy",
            "version": "1.0.0",
            "name": "dlstreams",
            "description": "Chaines live dlstreams (DaddyLive) proxifiees : headers + content-type corriges.",
            "resources": ["catalog", "meta", "stream"],
            "types": ["tv"],
            "idPrefixes": ["dlstreams:"],
            "catalogs": [{"type": "tv", "id": "dlstreams", "name": "dlstreams"}],
        }

    def _meta(self, c: dict) -> dict:
        return {"id": f"dlstreams:{c['id']}", "type": "tv", "name": c["name"],
                "poster": "", "posterShape": "landscape"}

def main():
    print(f"dlstreams addon+proxy sur http://0.0.0.0:{PORT}")
    print(f"  Stremio  : http://<ton-ip-LAN>:{PORT}/manifest.json")
    print(f"  VLC/mpv  : http://127.0.0.1:{PORT}/hls/121/index.m3u8")
    try:
        n = len(channels())
        print(f"  annuaire : {n} chaines chargees")
    except Exception as e:
        print(f"  annuaire : erreur de chargement ({e})")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()