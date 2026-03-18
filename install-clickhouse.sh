#!/bin/sh
set -e

CLICKHOUSE_VERSION="26.2.4.23"
DEST_DIR="./clickhouse-bin"
BINARY="${DEST_DIR}/clickhouse-${CLICKHOUSE_VERSION}"
SYMLINK="${DEST_DIR}/clickhouse-local"
BASE_URL="https://github.com/ClickHouse/ClickHouse/releases/download/v${CLICKHOUSE_VERSION}-stable"

OS=$(uname -s)
ARCH=$(uname -m)

mkdir -p "${DEST_DIR}"

if [ "${OS}" = "Linux" ]; then
    if [ "${ARCH}" = "x86_64" ] || [ "${ARCH}" = "amd64" ]; then
        TGZ_NAME="clickhouse-common-static-${CLICKHOUSE_VERSION}-amd64.tgz"
    elif [ "${ARCH}" = "aarch64" ] || [ "${ARCH}" = "arm64" ]; then
        TGZ_NAME="clickhouse-common-static-${CLICKHOUSE_VERSION}-arm64.tgz"
    else
        echo "Unsupported Linux architecture: ${ARCH}" >&2
        exit 1
    fi
    echo "Downloading ${BASE_URL}/${TGZ_NAME} ..."
    curl -fsSL "${BASE_URL}/${TGZ_NAME}" -o "/tmp/${TGZ_NAME}"
    tar -xzf "/tmp/${TGZ_NAME}" -C /tmp \
        "clickhouse-common-static-${CLICKHOUSE_VERSION}/usr/bin/clickhouse" \
        --strip-components=3
    mv "/tmp/clickhouse" "${BINARY}"
    rm -f "/tmp/${TGZ_NAME}"
elif [ "${OS}" = "Darwin" ]; then
    if [ "${ARCH}" = "x86_64" ] || [ "${ARCH}" = "amd64" ]; then
        MACOS_FILE="clickhouse-macos"
    elif [ "${ARCH}" = "aarch64" ] || [ "${ARCH}" = "arm64" ]; then
        MACOS_FILE="clickhouse-macos-aarch64"
    else
        echo "Unsupported macOS architecture: ${ARCH}" >&2
        exit 1
    fi
    echo "Downloading ${BASE_URL}/${MACOS_FILE} ..."
    curl -fsSL "${BASE_URL}/${MACOS_FILE}" -o "${BINARY}"
else
    echo "Unsupported operating system: ${OS}" >&2
    exit 1
fi

chmod +x "${BINARY}"
ln -sf "clickhouse-${CLICKHOUSE_VERSION}" "${SYMLINK}"

echo "Installed clickhouse-local ${CLICKHOUSE_VERSION} to ${SYMLINK}"
