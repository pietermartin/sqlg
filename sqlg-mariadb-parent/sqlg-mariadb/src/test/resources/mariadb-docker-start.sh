#!/bin/bash
docker run --name mariadb_main --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3306:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_modern --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3307:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_sink --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3308:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_grateful --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3309:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_empty --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3310:3306 -d docker.io/library/mariadb:latest

docker run --name mariadb_g1 --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3311:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_g2 --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3312:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_readGraph --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3313:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_standard --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3314:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_temp --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3315:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_temp1 --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3316:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_temp2 --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3317:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_subgraph --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3318:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_prototype --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3319:3306 -d docker.io/library/mariadb:latest
docker run --name mariadb_target --rm -e MYSQL_ROOT_PASSWORD=mariadb -p 3320:3306 -d docker.io/library/mariadb:latest
