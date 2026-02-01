* change pom to non snapshot version `mvn versions:set -DnewVersion=x.x.x`
* edit `<sqlg.version>x.x.x</sqlg.version>`
* mvn clean deploy -P release -DskipTests
