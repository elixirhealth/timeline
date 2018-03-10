#!/usr/bin/env bash

set -eou pipefail
#set -x  # useful for debugging

docker_cleanup() {
    echo "cleaning up existing network and containers..."
    CONTAINERS='timeline'
    docker ps | grep -E ${CONTAINERS} | awk '{print $1}' | xargs -I {} docker stop {} || true
    docker ps -a | grep -E ${CONTAINERS} | awk '{print $1}' | xargs -I {} docker rm {} || true
    docker network list | grep ${CONTAINERS} | awk '{print $2}' | xargs -I {} docker network rm {} || true
}

# optional settings (generally defaults should be fine, but sometimes useful for debugging)
TIMELINE_LOG_LEVEL="${TIMELINE_LOG_LEVEL:-INFO}"  # or DEBUG
TIMELINE_TIMEOUT="${TIMELINE_TIMEOUT:-5}"  # 10, or 20 for really sketchy network

# local and filesystem constants
LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# container command constants
TIMELINE_IMAGE="gcr.io/elxir-core-infra/timeline:snapshot" # develop

echo
echo "cleaning up from previous runs..."
docker_cleanup

echo
echo "creating timeline docker network..."
docker network create timeline

# TODO start and healthcheck dependency services if necessary

echo
echo "starting timeline..."
port=10100
name="timeline-0"
docker run --name "${name}" --net=timeline -d -p ${port}:${port} ${TIMELINE_IMAGE} \
    start \
    --logLevel "${TIMELINE_LOG_LEVEL}" \
    --serverPort ${port}
    # TODO add other relevant args if necessary
timeline_addrs="${name}:${port}"
timeline_containers="${name}"

echo
echo "testing timeline health..."
docker run --rm --net=timeline ${TIMELINE_IMAGE} test health \
    --addressses "${timeline_addrs}" \
    --logLevel "${TIMELINE_LOG_LEVEL}"

echo
echo "testing timeline ..."
docker run --rm --net=timeline ${TIMELINE_IMAGE} test io \
    --addresses "${timeline_addrs}" \
    --logLevel "${TIMELINE_LOG_LEVEL}"
    # TODO add other relevant args if necessary

echo
echo "cleaning up..."
docker_cleanup

echo
echo "All tests passed."
