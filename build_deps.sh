#!/usr/bin/env bash

function DownloadLib()
{
    if command -v apt-get &> /dev/null
    then
        echo "apt-get found"
        LIBPULSAR_PATH=/usr/lib
        if [[ -f "${LIBPULSAR_PATH}/libpulsar.a" ]]
        then
            echo "lib already downloaded"
            exit 0
        fi
        sudo apt-get update && sudo apt-get install -y --no-install-recommends \
            curl \
            ca-certificates \
            libssl-dev
        curl --show-error --silent --location "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${PULSAR_VERSION}/DEB/apache-pulsar-client.deb" --output apache-pulsar-client.deb
        curl --show-error --silent --location "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${PULSAR_VERSION}/DEB/apache-pulsar-client-dev.deb" --output apache-pulsar-client-dev.deb
        sudo apt-get install ./apache-pulsar-client*.deb
    elif command -v brew &> /dev/null
    then
        echo "brew found"
        LIBPULSAR_PATH=/usr/local/opt/libpulsar/lib
        if [[ -f "${LIBPULSAR_PATH}/libpulsar.a" ]]
        then
            # TODO: may need to check for latest version
            brew list --versions libpulsar
            echo "lib already downloaded"
            exit 0
        fi
        brew install libpulsar
    elif command -v rpm &> /dev/null
    then
        echo "rpm found"
        LIBPULSAR_PATH=/usr/lib
        if [[ -f "${LIBPULSAR_PATH}/libpulsar.a" ]]
        then
            echo "lib already downloaded"
            exit 0
        fi
        curl --show-error --silent --location "https://archive.apache.org/dist/pulsar/pulsar-${PULSAR_VERSION}/RPMS/apache-pulsar-client-${PULSAR_VERSION}-1.x86_64.rpm" --output apache-pulsar-client-${PULSAR_VERSION}-1.x86_64.rpm
        curl --show-error --silent --location "https://archive.apache.org/dist/pulsar/pulsar-${PULSAR_VERSION}/RPMS/apache-pulsar-client-devel-${PULSAR_VERSION}-1.x86_64.rpm" --output apache-pulsar-client-devel-${PULSAR_VERSION}-1.x86_64.rpm
        rpm -ivh apache-pulsar-client*.rpm
    else
        echo "no package manager found"
        exit 1
    fi
}
PULSAR_VERSION=2.7.0

DownloadLib
