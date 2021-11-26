FROM python:3.9
LABEL maintainer="neurocis <neurocis@neurocis.me>"

RUN true && \
\
# Install python prerequisites
pip3 install \
    grpcio==1.36.1 \
    paho-mqtt==1.5.1 \
    influxdb==5.3.1 python-dateutil==2.8.1 pytz==2021.1 requests==2.25.1 \
        certifi==2020.12.5 chardet==4.0.0 idna==2.10 urllib3==1.26.4 \
        six==1.15.0 msgpack==1.0.2 \
    influxdb_client==1.24.0 rx==3.2.0 \
    yagrc==1.1.1 grpcio-reflection==1.36.1 protobuf==3.15.6

ADD . /app
WORKDIR /app

# run crond as main process of container
ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]
CMD ["dish_grpc_influx.py status alert_detail"]

# docker run -d --name='starlink-grpc-tools' -e INFLUXDB_HOST=192.168.1.34 -e INFLUXDB_PORT=8086 -e INFLUXDB_DB=starlink
# --net='br0' --ip='192.168.1.39' neurocis/starlink-grpc-tools dish_grpc_influx.py status alert_detail
