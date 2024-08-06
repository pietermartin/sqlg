#!/bin/bash
docker run -d --rm --name mysql_main -e MYSQL_ROOT_PASSWORD=mysql -p 3306:3306 mysql
docker run -d --rm --name mysql_modern -e MYSQL_ROOT_PASSWORD=mysql -p 3307:3306 mysql
docker run -d --rm --name mysql_sink -e MYSQL_ROOT_PASSWORD=mysql -p 3308:3306 mysql
docker run -d --rm --name mysql_grateful -e MYSQL_ROOT_PASSWORD=mysql -p 3309:3306 mysql
docker run -d --rm --name mysql_empty -e MYSQL_ROOT_PASSWORD=mysql -p 3310:3306 mysql

docker run -d --rm --name mysql_g1 -e MYSQL_ROOT_PASSWORD=mysql -p 3311:3306 mysql
docker run -d --rm --name mysql_g2 -e MYSQL_ROOT_PASSWORD=mysql -p 3312:3306 mysql
docker run -d --rm --name mysql_readGraph -e MYSQL_ROOT_PASSWORD=mysql -p 3313:3306 mysql
docker run -d --rm --name mysql_standard -e MYSQL_ROOT_PASSWORD=mysql -p 3314:3306 mysql
docker run -d --rm --name mysql_temp -e MYSQL_ROOT_PASSWORD=mysql -p 3315:3306 mysql
docker run -d --rm --name mysql_temp1 -e MYSQL_ROOT_PASSWORD=mysql -p 3316:3306 mysql
docker run -d --rm --name mysql_temp2 -e MYSQL_ROOT_PASSWORD=mysql -p 3317:3306 mysql
docker run -d --rm --name mysql_subgraph -e MYSQL_ROOT_PASSWORD=mysql -p 3318:3306 mysql
docker run -d --rm --name mysql_prototype -e MYSQL_ROOT_PASSWORD=mysql -p 3319:3306 mysql
docker run -d --rm --name mysql_target -e MYSQL_ROOT_PASSWORD=mysql -p 3320:3306 mysql