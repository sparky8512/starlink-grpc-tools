#!/bin/sh

printenv >> /etc/environment
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
exec /usr/local/bin/python3 $@
