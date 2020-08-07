#!/usr/bin/env bash

# mostly taken from https://github.com/silviucpp/erlkaf/blob/master/build_deps.sh

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

  mkdir -p $MIX_DEPS_PATH
  pushd $MIX_DEPS_PATH

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
  pushd $MIX_DEPS_PATH
  pushd $DESTINATION

  OS=$(uname -s)

  case $OS in
        Darwin)
            brew install openssl protobuf boost boost-python boost-python3 log4cxx jsoncpp
            export OPENSSL_ROOT_DIR=$(brew --prefix openssl)
            export OPENSSL_INCLUDE_DIR=$OPENSSL_ROOT_DIR/include
            export OPENSSL_SSL_LIBRARY=$OPENSSL_ROOT_DIR/lib
            export CPPFLAGS=-I$OPENSSL_ROOT_DIR/include
            export LDFLAGS=-L$OPENSSL_ROOT_DIR/lib
            cd pulsar-client-cpp
            fail_check cmake . -DBUILD_TESTS=OFF -DBUILD_PYTHON_WRAPPER=OFF -DBoost_INCLUDE_DIRS=$(brew --prefix boost)/include -DProtobuf_INCLUDE_DIR=$(brew --prefix protobuf)/include -DProtobuf_LIBRARIES=$(brew --prefix protobuf)/lib/libprotobuf.dylib
            fail_check make pulsarShared -j$(nproc)
            ;;
        *)
            cd pulsar-client-cpp
            fail_check cmake . -DBUILD_TESTS=OFF -DLINK_STATIC=ON
            fail_check make pulsarStatic -j$(nproc)
  esac

  popd
  popd
}

DESTINATION=pulsar

if [[ -f "$MIX_DEPS_PATH/$DESTINATION/pulsar-client-cpp/lib/libpulsar.a" || -f "$MIX_DEPS_PATH/$DESTINATION/pulsar-client-cpp/lib/libpulsar.dylib" ]]; then
    echo "pulsar fork already exist. delete $MIX_DEPS_PATH/$DESTINATION for a fresh checkout."
    exit 0
fi

REPO=https://github.com/apache/pulsar
BRANCH=v2.6.0
REV=653ef409e5a3e72d7a7917d45b54c46e7bff5c16

DownloadLib
BuildLib
