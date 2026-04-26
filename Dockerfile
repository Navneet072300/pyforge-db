FROM python:3.11-slim

WORKDIR /app

COPY forge/ forge/
COPY strata/ strata/
COPY volt/ volt/

RUN mkdir -p /data/forge /data/strata /data/volt

# Default: Volt TCP server (override CMD for other engines)
CMD ["python3", "-m", "volt", "--server", "--host", "0.0.0.0", "--port", "6399"]
