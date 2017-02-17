#!/bin/bash

exec java -server -d64 -Xms500m -Xmx500m -Dfile.encoding=UTF-8 \
    -Djava.net.preferIPv4Stack=true -XX:+UseParNewGC -XX:+UseConcMarkSweepGC \
    -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly \
    -XX:MaxGCPauseMillis=150 -XX:InitiatingHeapOccupancyPercent=70 \
    -XX:NewRatio=2 -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=15 \
    -XX:+UseCompressedOops -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -XX:-MaxFDLimit \
    -Dio.netty.leakDetectionLevel=disabled \
    -cp /usr/share/taskflow/lib/taskflow.jar \
    org.github.mitallast.taskflow.Main