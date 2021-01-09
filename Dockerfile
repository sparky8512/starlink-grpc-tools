FROM python:3.9
LABEL maintainer="neurocis <neurocis@neurocis.me>"

RUN true && \
# Install package prerequisites
apt-get update && \
apt-get install -qy cron && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
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
CMD true && \
printenv >> /etc/environment && \
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
#ntpd -p pool.ntp.org && \
grpcurl -plaintext -protoset-out dish.protoset 192.168.100.1:9200 describe SpaceX.API.Device.Device && \
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/device.proto && \
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/common/status/status.proto && \
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/command.proto && \
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/common.proto && \
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/dish.proto && \
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/wifi.proto && \
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/wifi_config.proto && \
echo "$CRON_ENTRY" | crontab - && cron -f

# docker run -d --name='starlink-grpc-tools' -e INFLUXDB_HOST=192.168.1.34 -e INFLUXDB_PORT=8086 -e INFLUXDB_DB=starlink 
# -e "CRON_ENTRY=* * * * * /usr/local/bin/python3 /app/dishStatusInflux_cron.py > /proc/1/fd/1 2>/proc/1/fd/2"
# --net='br0' --ip='192.168.1.39' neurocis/starlink-grpc-tools
