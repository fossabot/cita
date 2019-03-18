#!/usr/bin/env bash

SOURCE_DIR="$(cd $(dirname "$0") && pwd -P)"

test -f "${SOURCE_DIR}/CODE_OF_CONDUCT.md"
if [ $? -eq 0 ]; then
    CONTAINER_NAME="cita_build_container"
    DOCKER_IMAGE="cita/cita-build:ubuntu-18.04-20190304"
else
    CONTAINER_NAME="cita_run_container"
    DOCKER_IMAGE="cita/cita-run:ubuntu-18.04-20181009"
    SOURCE_DIR="$(dirname $SOURCE_DIR)"
fi

if [ `uname` == 'Darwin' ]; then
    cp /etc/localtime ${SOURCE_DIR}/localtime
    SYSTEM_NET="bridge"
    LOCALTIME_PATH="${SOURCE_DIR}/localtime"
else
    SYSTEM_NET="host"
    LOCALTIME_PATH="/etc/localtime"
fi

WORKDIR=/opt/cita
USER_ID=`id -u $USER`
USER_NAME="user"

[[ "${USER_ID}" = "0" ]] && USER_NAME="root"

# Init contanier's cargo, for logs.
CARGO_HOME=/opt/.cargo
DOCKER_CARGO=${HOME}/.docker_cargo
mkdir -p ${DOCKER_CARGO}/git

# Port condition
EXPOSE="1337:1337"
if [ "$3" == "port" ]; then
    EXPOSE=${@:4}
    [[ "${EXPOSE}" = "NULL" ]] && EXPOSE="1337:1337"
    docker container stop $CONTAINER_NAME > /dev/null 2>&1
fi

# Run container
docker ps | grep ${CONTAINER_NAME} > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Start docker container ${CONTAINER_NAME} ..."
    docker rm ${CONTAINER_NAME} > /dev/null 2>&1

    docker run -d \
           --net=${SYSTEM_NET} \
           --volume ${SOURCE_DIR}:${WORKDIR} \
           --volume ${DOCKER_CARGO}/git:${DOCKER_CARGO}/git \
           --volume ${LOCALTIME_PATH}:/etc/localtime \
           --env USER_ID=${USER_ID} \
           --workdir ${WORKDIR} \
           --name ${CONTAINER_NAME} \
           -p $EXPOSE ${DOCKER_IMAGE} \
           /bin/bash -c "while true;do sleep 100;done"
    # Wait entrypoint.sh to finish
    sleep 3
fi

test -t 1 && USE_TTY="-t"

# Start nodes outside container directly
# if `daemon.sh` just use in start node?
# Remote Commands:
# $0 - realpath, $1 - cita_env $2 - bebop,
# $3 - command/daemon, $4 - .../command, $5 - ...
[[ "$3" == "start" ]] && set "${@:0:3}" "daemon" "${@:3}"

# Condition `daemon` to run daemon.
if [ "$3" == "daemon"  ]; then
    set "${@:0:3}" "${@:4}"
    docker exec -d ${CONTAINER_NAME} /usr/bin/gosu ${USER_NAME} "$@"
elif [ $# -gt 0 ]; then
    docker exec -i ${USE_TTY} ${CONTAINER_NAME} /usr/bin/gosu ${USER_NAME} "$@"
else
    docker exec -i ${USE_TTY} ${CONTAINER_NAME} \
        /bin/bash -c "stty cols $(tput cols) rows $(tput lines) && /usr/bin/gosu ${USER_NAME} /bin/bash"
fi
