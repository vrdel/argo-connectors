#!/bin/bash

CONNECTORS_SOURCE="${HOME}/my_work/srce/git.argo-connectors/argo-connectors"

docker rm connectors-r9; \
docker run \
--log-driver json-file \
--log-opt max-size=10m \
--name connectors-r9 \
--privileged \
--net host \
--privileged \
-v "${HOME}":/mnt/ \
-v "${HOME}"/.ssh:/home/user/.ssh/ \
-v "${CONNECTORS_SOURCE}":/home/user/connectors-source \
\
-h docker-rocky9 \
--rm -ti -v /dev/log:/dev/log ipanema:5000/connectors-r9
