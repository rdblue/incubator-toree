#!/bin/bash

set -e

export NETFLIX_ENVIRONMENT=prod

TOREE_VERSION=0.2.0
BUILD_VERSION=${BUILD_VERSION:-${TOREE_VERSION}-unstable}

${NEBULA_HOME}/gradlew toree-assembly:build

# use the Toree version instead of the build version so that the Genie setup
# script is the same for unstable and release.
STAGING_DIR=toree-${TOREE_VERSION}
TARBALL=toree-${TOREE_VERSION}.tar.gz

[ -d ${STAGING_DIR} ] && rm -r ${STAGING_DIR} || true
mkdir ${STAGING_DIR}
cp toree-assembly/build/libs/toree-assembly-${TOREE_VERSION}.jar ${STAGING_DIR}
cp netflix/run.py ${STAGING_DIR}
cp netflix/run-console.py ${STAGING_DIR}

tar czf ${TARBALL} ${STAGING_DIR}

s3cp -k BDP_JENKINS_KEY ${TARBALL} s3://netflix-bigdataplatform/apps/toree/${BUILD_VERSION}/toree-${TOREE_VERSION}-build-${BUILD_ID}.tar.gz
s3cp -k BDP_JENKINS_KEY ${TARBALL} s3://netflix-bigdataplatform/apps/toree/${BUILD_VERSION}/${TARBALL}
