#!/bin/bash -x

POD_ID=$(echo $HOSTNAME|sed -re 's/.*-(.{5})$/\1/g')
export METRICS_TAGS="j_site=$TUG_CLUSTER,j_service=$TUG_PROJECT/$TUG_APP,j_pod_id=$POD_ID"
set

exec "/usr/bin/kafka_httpcat"
