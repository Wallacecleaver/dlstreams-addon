# W Addon TV -> Stremio : addon + proxy autonome.
# Zero dependance (stdlib pure) -> image minimale, rien a installer.
   FROM python:3.12-slim
   WORKDIR /app
   COPY dlstreams_addon.py .
   COPY configure.html .      <-- AJOUTE ÇA
   COPY dashboard.html .      <-- AJOUTE ÇA
   EXPOSE 8781
   CMD ["python", "dlstreams_addon.py"]