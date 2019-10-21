#!/bin/bash

set -x

export JDK_OS=darwin
export JAVA_HOME=~/.jenv/versions/12.0
export DDLOG=~/Documents/DCM/ddlog

cd ~/Documents/DCM/differential-datalog

ddlog -i test/datalog_tests/weave_fewer_queries_cap.dl -L lib -j
cd test/datalog_tests/weave_fewer_queries_cap_ddlog
cargo build --features=flatbuf --release

cc -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I. -I${DDLOG}/lib ${DDLOG}/java/ddlogapi.c -Ltarget/release/ -lweave_fewer_queries_cap_ddlog -o libddlogapi.dylib

export CLASSPATH=/Users/fariakalim/Documents/DCM/ddlog/java/ddlogapi.jar:.:/Users/fariakalim/Documents/DCM/flatbuffers-java-1.11.0.jar:$CLASSPATH

cd ~/Documents/DCM/differential-datalog/test/datalog_tests/weave_fewer_queries_cap_ddlog/flatbuf/java/
javac ddlog/__weave_fewer_queries_cap/*.java
javac ddlog/weave_fewer_queries_cap/*.java

jar -cf weave-apps.jar ddlog/*

mvn install:install-file -Dfile=weave-apps.jar -DgroupId=ddlog.weave_fewer_queries_cap -DartifactId="ddlog.weave_fewer_queries_cap" -Dversion=0.1 -Dpackaging=jar

mvn install:install-file -Dfile=/Users/fariakalim/Documents/DCM/ddlog/java/ddlogapi.jar -DgroupId=ddlogapi -DartifactId=ddlog -Dversion=1.0 -Dpackaging=jar
