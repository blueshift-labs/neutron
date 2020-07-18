# mostly taken from https://github.com/silviucpp/erlkaf/blob/master/build_deps.sh

#!/usr/bin/env bash

function fail_check()
{
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error with $1" >&2
        exit 1
    fi
}

function DownloadLib()
{
  echo "repo=$REPO rev=$REV branch=$BRANCH"

  mkdir -p $DEPS_LOCATION
  pushd $DEPS_LOCATION

  if [ ! -d "$DESTINATION" ]; then
      fail_check git clone -b $BRANCH $REPO $DESTINATION
  fi

  pushd $DESTINATION
  fail_check git checkout $REV
  popd
  popd
}

function BuildLib()
{
  pushd $DEPS_LOCATION
  pushd $DESTINATION

  OS=$(uname -s)

  case $OS in
        Darwin)
            brew install openssl protobuf boost boost-python log4cxx jsoncpp
            export OPENSSL_ROOT_DIR=$(brew --prefix openssl)
            export OPENSSL_INCLUDE_DIR=$OPENSSL_ROOT_DIR/include
            export OPENSSL_SSL_LIBRARY=$OPENSSL_ROOT_DIR/lib
            export CPPFLAGS=-I$OPENSSL_ROOT_DIR/include
            export LDFLAGS=-L$OPENSSL_ROOT_DIR/lib
            ;;
  esac

  cd pulsar-client-cpp
  fail_check cmake . -DBUILD_TESTS=OFF
  fail_check make -j$(nproc)

  popd
  popd
}

DEPS_LOCATION=deps
DESTINATION=pulsar

if [ -f "$DEPS_LOCATION/$DESTINATION/pulsar-client-cpp/lib/libpulsar.a" ]; then
    echo "pulsar fork already exist. delete $DEPS_LOCATION/$DESTINATION for a fresh checkout."
    exit 0
fi

REPO=https://github.com/apache/pulsar
BRANCH=v2.6.0
REV=653ef409e5a3e72d7a7917d45b54c46e7bff5c16

DownloadLib
BuildLib
