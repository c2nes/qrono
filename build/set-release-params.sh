#!/bin/bash

set -euo pipefail

# Required variables:
#  GITHUB_REF
#  GITHUB_RUN_NUMBER
# See https://docs.github.com/en/free-pro-team@latest/actions/reference/environment-variables#default-environment-variables

REPO=qrono/qrono

case "$GITHUB_REF" in
    refs/tags/[0-9]*)
        TAG=${GITHUB_REF#refs/tags/}
        IFS=. read MAJOR MINOR REST <<< "$TAG"
        echo "docker_push=true"
        TAGS="${REPO}:${TAG}"
        if [ "${MAJOR}.${MINOR}" != "0.0" ]; then
            TAGS="${TAGS},${REPO}:${MAJOR}.${MINOR}"
        fi
        if [ "$MAJOR" != "0" ]; then
            TAGS="${TAGS},${REPO}:${MAJOR}"
        fi
        TAGS="${TAGS},${REPO}:latest"
        echo "docker_tags=$TAGS"
        ;;

    refs/heads/main)
        echo "docker_push=true"
        TAG="dev-$(date +%Y%m%d)-$GITHUB_RUN_NUMBER"
        TAGS="${REPO}:${TAG}"
        TAGS="${TAGS},${REPO}:dev"
        echo "docker_tags=$TAGS"
        ;;

    *)
        echo "docker_push=false"
        echo "docker_tags=${REPO}:dev"
        ;;
esac
