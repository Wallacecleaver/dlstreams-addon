# W Addon TV → Stremio (addon + proxy autonome)

Récupère les chaînes live de **dlstreams.st** (réseau DaddyLive) et les sert :

- comme **addon Stremio** (catalogue + lecture) ;
- comme **proxy HLS** jouable dans VLC / mpv / ffmpeg.

Autonome : **aucune dépendance, aucune clé, aucun compte**. Le proxy intégré injecte
les en-têtes requis et corrige le `Content-Type` des segments — donc **pas besoin de
MediaFlow ni de quoi que ce soit d'autre**.

## Lancer

### Docker (recommandé)

```bash
docker compose up -d --build
```

### Ou sans Docker (Python 3.8+ suffit)

```bash
python3 dlstreams_addon.py
```

## Utiliser

- **Stremio** : Addons → « Install via URL » →
  `http://<IP-de-la-machine>:8781/manifest.json`
  (mets l'IP LAN de la machine, pas `127.0.0.1`, si Stremio tourne ailleurs).
  Chaque chaîne propose **plusieurs serveurs** (`🔀 Auto` + Player 1…N) : si un
  serveur coupe, reviens à la liste et prends-en un autre.

- **VLC / mpv / ffmpeg** : ouvre directement une chaîne par son id :
  `http://127.0.0.1:8781/hls/121/index.m3u8`         (Auto / failover)
  `http://127.0.0.1:8781/hls/121/p0/index.m3u8`       (Player 1 précis)

## Notes

- Les chaînes sont des **directs** (souvent des événements) : un id peut être
  hors-antenne à un instant donné → il rejoue quand la diffusion reprend.
- Changer le port : `PORT=9000 python3 dlstreams_addon.py` (et adapter le mapping
  dans `docker-compose.yml`).
- C'est une **base de travail** : le code (`dlstreams_addon.py`) est court et
  commenté, à étendre librement (posters, filtrage, EPG…).
