#!/bin/sh

export MAVEN_OPTS=" -Djava.security.egd=file:/dev/./urandom -XX:+TieredCompilation -XX:TieredStopAtLevel=1"

#mvn -T40 -q test
mvn -T40 -e -X test
