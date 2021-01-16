FROM python:3.9
LABEL maintainer="neurocis <neurocis@neurocis.me>"

RUN true && \
\
# Install GRPCurl
wget https://github.com/fullstorydev/grpcurl/releases/download/v1.8.0/grpcurl_1.8.0_linux_x86_64.tar.gz && \
tar -xvf grpcurl_1.8.0_linux_x86_64.tar.gz grpcurl && \
chown root:root grpcurl && \
chmod 755 grpcurl && \
mv grpcurl /usr/bin/. && \
rm grpcurl_1.8.0_linux_x86_64.tar.gz && \
\
# Install python prerequisites
pip3 install grpcio grpcio-tools paho-mqtt influxdb

ADD . /app
WORKDIR /app

# run crond as main process of container
ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]
CMD ["dishStatusInflux.py", "-t", "30"]

# docker run -d --name='starlink-grpc-tools' -e INFLUXDB_HOST=192.168.1.34 -e INFLUXDB_PORT=8086 -e INFLUXDB_DB=starlink
# --net='br0' --ip='192.168.1.39' neurocis/starlink-grpc-tools dishStatusInflux.py -t 30
