#!/bin/bash

set -ex

# assumptions: 1) weave_queries_order_cap is in ddlog folder
# 2) path variables are set 
export JDK_OS=darwin
export JAVA_HOME=~/.jenv/versions/12.0
export DDLOG=~/Documents/DCM/ddlog
export DPROG=weave_fewer_queries_cap
#export DPROG=weave_queries_order_cap
cd ${DDLOG}

ddlog -i ${DPROG}.dl -L lib -j
cp Cargo.lock ${DPROG}_ddlog/
cd ${DPROG}_ddlog
cargo build --features=flatbuf --release

cc -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I. -I${DDLOG}/lib ${DDLOG}/java/ddlogapi.c -Ltarget/release/ -l${DPROG}_ddlog -o libddlogapi.dylib

export CLASSPATH=${DDLOG}/java/ddlogapi.jar:.:~/Documents/DCM/flatbuffers-java-1.11.0.jar:$CLASSPATH

cd ${DDLOG}/${DPROG}_ddlog/flatbuf/java/
javac ddlog/__${DPROG}/*.java
javac ddlog/${DPROG}/*.java

jar -cf weave-apps.jar ddlog/*

mvn install:install-file -Dfile=weave-apps.jar -DgroupId=ddlog.${DPROG} -DartifactId=ddlog.${DPROG} -Dversion=0.1 -Dpackaging=jar

mvn install:install-file -Dfile=${DDLOG}/java/ddlogapi.jar -DgroupId=ddlogapi -DartifactId=ddlog -Dversion=1.0 -Dpackaging=jar
