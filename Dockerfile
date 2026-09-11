FROM python:3.12
RUN python3 -m pip install --upgrade pip

# Detect architecture and set grpcio build flags
RUN ARCH="$(uname -m)" && \
    if [ "$ARCH" = "armv7l" ]; then \
        NOBIN_OPT="--no-binary=grpcio"; \
    else \
        NOBIN_OPT=""; \
    fi && \
    pip3 install --no-cache-dir $NOBIN_OPT \
        croniter==6.0.0 \
        pytz==2025.2 \
        six==1.17.0 \
        grpcio==1.76.0 \
        influxdb==5.3.2 \
        certifi==2026.1.4 \
        charset-normalizer==3.4.4 \
        idna==3.11 \
        msgpack==1.1.2 \
        requests==2.32.5 \
        urllib3==2.6.3 \
        influxdb-client==1.50.0 \
        reactivex==4.1.0 \
        paho-mqtt==2.1.0 \
        pypng==0.20220715.0 \
        python-dateutil==2.9.0.post0 \
        typing_extensions==4.15.0 \
        yagrc==1.1.2 \
        grpcio-reflection==1.76.0 \
        protobuf==6.33.4

# Copy application files
# Copy all Python application files
COPY ./*.py /app/

WORKDIR /app

# Remove entrypoint.sh entirely — it is causing your crash
# ENTRYPOINT is not needed for this project

CMD ["python", "main.py", "status", "alert_detail"]

LABEL maintainer="sike42"
