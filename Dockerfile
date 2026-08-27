# W Addon TV -> Stremio : addon + proxy autonome.
# Zero dependance (stdlib pure) -> image minimale, rien a installer.
FROM python:3.12-slim
WORKDIR /app
COPY dlstreams_addon.py .
COPY dashboard.html .
COPY configure.html .
COPY wiseplay.html .
# Assets curés (logos par catégorie + posters) — indispensables au mapping logos/catégories.
COPY LOGOS ./LOGOS
COPY POSTER ./POSTER
EXPOSE 8781
# Port configurable : -e PORT=8781
CMD ["python", "dlstreams_addon.py"]