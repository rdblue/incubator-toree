#!/bin/bash

set -e

if ! grep 'sbt' /etc/apt/sources.list; then
  echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
  sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
fi

sudo apt-get update -y
sudo apt-get install sbt -y

export NETFLIX_ENVIRONMENT=prod

TOREE_VERSION=0.2.0
BUILD_VERSION=${BUILD_VERSION:-${TOREE_VERSION}-unstable}

IS_SNAPSHOT=false VERSION=${TOREE_VERSION} sbt clean toree/assembly

# use the Toree version instead of the build version so that the Genie setup
# script is the same for unstable and release.
STAGING_DIR=toree-${TOREE_VERSION}
TARBALL=toree-${TOREE_VERSION}.tar.gz

rm -r ${STAGING_DIR}
mkdir ${STAGING_DIR}
cp target/scala-2.11/toree-assembly-${TOREE_VERSION}.jar ${STAGING_DIR}
cp netflix/run.py ${STAGING_DIR}

tar czf ${TARBALL} ${STAGING_DIR}

s3cp -k BDP_JENKINS_KEY ${TARBALL} s3://netflix-bigdataplatform/apps/toree/${BUILD_VERSION}/toree-${TOREE_VERSION}-build-${BUILD_ID}.tar.gz
s3cp -k BDP_JENKINS_KEY ${TARBALL} s3://netflix-bigdataplatform/apps/toree/${BUILD_VERSION}/${TARBALL}
