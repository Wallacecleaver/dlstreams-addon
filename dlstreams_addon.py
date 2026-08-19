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
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8781"))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
SITE = "https://dlstreams.st"                 # wrapper : sa home = l'annuaire, ses pages = le pointeur DaddyLive
_CH_TTL = 1800                                # cache annuaire 30 min


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
    """Annuaire dlstreams -> [{id, name}]. Cache 30 min. Dedoublonne par id (garde le 1er nom)."""
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
    out = [{"id": i, "name": n} for i, n in seen.items()]
    _ch_cache.update(at=now, list=out)
    return out


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


def working_players(cid: str) -> list[tuple[int, str]]:
    """[(index, label)] des players qui resolvent REELLEMENT un flux (verifies en parallele) -> on ne
    propose que les serveurs jouables, pas de boutons morts. L'index sert a l'URL /hls/<id>/p<index>."""
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


# ---------------------------------------------------------------- proxy HLS (headers + fix content-type)
# ---------------------------------------------------------------- Vavoo (protocole mediahubmx)
# Meme protocole que l'app Onyx/VYPN. La signature est FETCHEE (pas calculee) -> aucune crypto.
# Le flux Vavoo est du HLS qui exige juste User-Agent: MediaHubMX/2 -> le proxy l'injecte, sans MFP.
_VAVOO_MIRRORS = (("https://oha.cx", "mediaurl", False),          # sert le VRAI flux (pas de signature)
                  ("http://178.239.115.119", "mediaurl", False),  # meme service, repli DNS
                  ("https://vavoo.net", "mediahubmx", True),      # repli : ne sert que la mire promo
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
    req = urllib.request.Request(url, data=data, headers=h, method="POST")   # noqa: S310
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
    """Catalogue Vavoo d'un pays -> [{id, name, logo}] (id = url mediahubmx opaque). Cache 6 h, pagine."""
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
    """GET une URL avec un jeu de headers arbitraire (UA par defaut si absent). Chaque source porte
    SON profil : Referer/Origin (dlstreams), User-Agent MediaHubMX/2 (Vavoo)."""
    headers = {"User-Agent": UA}
    headers.update(hdr or {})
    req = urllib.request.Request(url, headers=headers)                       # noqa: S310
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _rewrite_playlist(text: str, playlist_url: str, hdr_enc: str, self_base: str) -> str:
    """Reecrit une playlist HLS : chaque URL (sous-playlist .m3u8 ou segment) passe par NOTRE proxy,
    en propageant le jeu de headers `hdr_enc` (b64 JSON) -> le proxy refetch avec les bons en-tetes."""
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
                # /catalog/tv/<source>.json  OU  /catalog/tv/<source>/search=xxx&skip=100.json
                extra = path[len("/catalog/tv/"):].removesuffix(".json")
                catid = extra.split("/", 1)[0]                   # "dlstreams" | "vavoo"
                params = {}
                if "/" in extra:
                    for kv in extra.split("/", 1)[1].split("&"):
                        if "=" in kv:
                            k, v = kv.split("=", 1)
                            params[k] = urllib.parse.unquote_plus(v)
                chans = vavoo_channels() if catid == "vavoo" else channels()
                q = params.get("search", "").lower().strip()
                if q:                                            # tous les mots présents (le « + » de
                    words = q.replace("+", " ").split()          # « Canal+ Foot » ne casse plus la recherche)
                    chans = [c for c in chans
                             if all(w in c["name"].lower() for w in words)]
                skip = int(params.get("skip") or 0)
                metas = [self._meta(c, catid) for c in chans[skip:skip + 100]]   # pagination (100/page)
                return self._send(200, json.dumps({"metas": metas}).encode(), "application/json", True)
            if path.startswith("/meta/tv/"):
                # Stremio percent-encode le ':' de l'id (vavoo%3A...) -> décoder avant de parser.
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
                seg = urllib.parse.unquote(path.rsplit("/", 1)[1].removesuffix(".json"))  # ':' encodé %3A
                source, _, cid = seg.partition(":")
                b = self._self_base()
                if source == "vavoo":                            # 1 flux (Vavoo n'a pas de multi-serveur)
                    streams = [{"name": "Vavoo", "title": "🔴 Direct", "url": f"{b}/vhls?v={cid}"}]
                    return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")
                ok = working_players(cid)                        # dlstreams : multi-player, que les vivants
                streams = [{"name": "dlstreams", "title": "🔀 Auto (1er dispo)",
                            "url": f"{b}/hls/{cid}/index.m3u8"}] if ok else []
                streams += [{"name": "dlstreams", "title": label,
                             "url": f"{b}/hls/{cid}/p{i}/index.m3u8"} for i, label in ok]
                return self._send(200, json.dumps({"streams": streams}).encode(), "application/json")
            # ---- proxy HLS ---- (headers par source : dlstreams=Referer/Origin, Vavoo=UA MediaHubMX/2)
            if path.startswith("/hls/") and path.endswith("/index.m3u8"):    # dlstreams
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
            if path == "/vhls":                                  # Vavoo : résout puis proxifie en HLS
                real = vavoo_resolve(_unb64u(qs["v"][0]))
                if not real:
                    return self._send(502, b"vavoo: flux introuvable (hors-antenne ?)", "text/plain")
                hdr = {"User-Agent": _VAVOO_UA}
                henc = _b64u(json.dumps(hdr))
                text = _proxy_get(real, hdr).decode("utf-8", "replace")
                return self._send(200, _rewrite_playlist(text, real, henc, self._self_base()).encode(),
                                  "application/vnd.apple.mpegurl")
            if path == "/px":                                    # sous-playlist -> refetch (headers) + reecrit
                url = _unb64u(qs["u"][0])
                henc = qs.get("h", [""])[0]
                hdr = json.loads(_unb64u(henc)) if henc else {}
                text = _proxy_get(url, hdr).decode("utf-8", "replace")
                return self._send(200, _rewrite_playlist(text, url, henc, self._self_base()).encode(),
                                  "application/vnd.apple.mpegurl")
            if path == "/sx":                                    # segment -> refetch (headers) + rebalise TS
                url = _unb64u(qs["u"][0])
                henc = qs.get("h", [""])[0]
                hdr = json.loads(_unb64u(henc)) if henc else {}
                return self._send(200, _proxy_get(url, hdr), "video/mp2t")
            return self._send(404, b"not found", "text/plain")
        except Exception as e:                                   # noqa: BLE001 -- un flux mort ne tue pas le serveur
            return self._send(502, f"resolve/proxy error: {type(e).__name__}: {e}".encode(), "text/plain")

    # ---- helpers addon ----
    def _manifest(self) -> dict:
        _extra = [{"name": "search", "isRequired": False}, {"name": "skip", "isRequired": False}]
        return {
            "id": "st.dlstreams.proxy",
            "version": "1.1.0",
            "name": "dlstreams + Vavoo",
            "description": "Chaines live dlstreams (DaddyLive) + Vavoo, proxifiees (headers + content-type "
                           "corriges). Sans MediaFlow.",
            "resources": ["catalog", "meta", "stream"],
            "types": ["tv"],
            "idPrefixes": ["dlstreams:", "vavoo:"],
            "catalogs": [{"type": "tv", "id": "dlstreams", "name": "dlstreams",
                          "extra": _extra, "extraSupported": ["search", "skip"]},
                         {"type": "tv", "id": "vavoo", "name": "Vavoo",
                          "extra": _extra, "extraSupported": ["search", "skip"]}],
        }

    def _meta(self, c: dict, source: str) -> dict:
        # dlstreams : id numerique ; vavoo : url mediahubmx opaque -> b64 dans l'id Stremio.
        cid = c["id"] if source == "dlstreams" else _b64u(c["id"])
        logo = c.get("logo") or ""
        return {"id": f"{source}:{cid}", "type": "tv", "name": c["name"],
                "poster": logo, "logo": logo, "posterShape": "landscape"}


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
