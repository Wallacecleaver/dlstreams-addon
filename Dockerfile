# W Addon TV -> Stremio : addon + proxy autonome.
# Zero dependance (stdlib pure) -> image minimale, rien a installer.
FROM python:3.12-slim
WORKDIR /app
COPY dlstreams_addon.py .
EXPOSE 8781
# Port configurable : -e PORT=8781
CMD ["python", "dlstreams_addon.py"]
