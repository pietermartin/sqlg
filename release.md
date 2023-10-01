* change pom to non snapshot version
* mvn clean deploy -P release -DskipTests
* log in to https://oss.sonatype.org
* close and release the artifacts
* tag with pom version
* might need to add `export GPG_TTY=$(tty)`
* copy generated docs to `/user/share/nginx/html`
