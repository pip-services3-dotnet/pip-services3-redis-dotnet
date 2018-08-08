#!/bin/bash

VERSION=$(grep -m1 version componentBenchmarks.json | tr -d '\r' | awk -F: '{ print $2 }' | sed 's/[", ]//g')
BUILD_NUMBER=$(grep -m1 build_number componentBenchmarks.json | tr -d '\r' | awk -F: '{ print $2 }' | sed 's/[", ]//g')
IMAGE_NAME=$(grep -m1 image componentBenchmarks.json | tr -d '\r' | awk -F: '{ print $2 }' | sed 's/[", ]//g')

IMAGE="${IMAGE_NAME}:latest"

# Any subsequent(*) commands which fail will cause the shell scrupt to exit immediately
set -e
set -o pipefail

docker build -f docker/Dockerfile.benchmark -t ${IMAGE} .

# Push production image to azure container registry
# docker login ${DOCKER_REGISTRY_SERVER} -u ${DOCKER_REGISTRY_USERNAME} -p ${DOCKER_REGISTRY_PASSWORD}

docker login devbootbarn.azurecr.io -u devbootbarn -p dr6Hc87sjvAC7ofQ=PIckxRUFPNUq2E8

docker push $IMAGE