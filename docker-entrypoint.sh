#!/bin/bash -x

POD_ID=$(echo $HOSTNAME|sed -re 's/.*-(.{5})$/\1/g')
export METRICS_TAGS="j_site=$TUG_CLUSTER,j_service=$TUG_PROJECT/$TUG_APP,j_pod_id=$POD_ID"
set

exec "kafka_httpcat kafka-2.local.jumpy:9092 tug_metrics_kafka2 metrics"
