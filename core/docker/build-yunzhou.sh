#!/usr/bin/env bash

set -euxo pipefail

SOURCE_DIR="../.."

# Retrieve the script directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "${SCRIPT_DIR}" || exit 2

TRINO_VERSION=$("${SOURCE_DIR}/mvnw" -f "${SOURCE_DIR}/pom.xml" --quiet help:evaluate -Dexpression=project.version -DforceStdout)
echo "🎯 Using currently built artifacts from the core/trino-server and client/trino-cli modules and version ${TRINO_VERSION}"
arch="amd64"

WORK_DIR="$(mktemp -d)"
cp "${SOURCE_DIR}/core/trino-server/target/trino-server-${TRINO_VERSION}.tar.gz" "${WORK_DIR}"
tar -C "${WORK_DIR}" -xzf "${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz"
rm "${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz"
cp -R bin "${WORK_DIR}/trino-server-${TRINO_VERSION}"
cp -R default "${WORK_DIR}/"

cp "${SOURCE_DIR}/client/trino-cli/target/trino-cli-${TRINO_VERSION}-executable.jar" "${WORK_DIR}"

cp "${SOURCE_DIR}/core/docker/tools/async-profiler-3.0-linux-x64.tar.gz" "${WORK_DIR}"
tar -C "${WORK_DIR}" -xzf "${WORK_DIR}/async-profiler-3.0-linux-x64.tar.gz"
rm "${WORK_DIR}/async-profiler-3.0-linux-x64.tar.gz"

CONTAINER="trino:${TRINO_VERSION}"

docker build "${WORK_DIR}" --pull --platform "linux/${arch}" -f DockerfileYunzhou -t "${CONTAINER}" --build-arg "TRINO_VERSION=${TRINO_VERSION}"

rm -r "${WORK_DIR}"

# Source common testing functions
source container-test.sh

test_container "${CONTAINER}" "linux/${arch}"
docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' "${CONTAINER}"
