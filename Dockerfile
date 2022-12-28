FROM python:3.9
LABEL maintainer="neurocis <neurocis@neurocis.me>"

RUN true && \
\
ARCH=`uname -m`; \
if [ "$ARCH" = "armv7l" ]; then \
    NOBIN_OPT="--no-binary=grpcio"; \
else \
    NOBIN_OPT=""; \
fi; \
# Install python prerequisites
pip3 install --no-cache-dir $NOBIN_OPT \
    grpcio==1.50.0 six==1.16.0 \
    influxdb==5.3.1 certifi==2022.9.24 charset-normalizer==2.1.1 idna==3.4 \
        msgpack==1.0.4 python-dateutil==2.8.2 pytz==2022.6 requests==2.28.1 \
        urllib3==1.26.12 \
    influxdb-client==1.34.0 reactivex==4.0.4 \
    paho-mqtt==1.6.1 \
    pypng==0.20220715.0 \
    typing_extensions==4.4.0 \
    yagrc==1.1.1 grpcio-reflection==1.50.0 protobuf==4.21.9

COPY dish_*.py starlink_*.py entrypoint.sh /app/
WORKDIR /app

ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]
CMD ["dish_grpc_influx.py status alert_detail"]

# docker run -d --name='starlink-grpc-tools' -e INFLUXDB_HOST=192.168.1.34 -e INFLUXDB_PORT=8086 -e INFLUXDB_DB=starlink
# --net='br0' --ip='192.168.1.39' ghcr.io/sparky8512/starlink-grpc-tools dish_grpc_influx.py status alert_detail
