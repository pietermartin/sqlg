--as postgres user
#as root
cd /var/lib/postgres/
mkdir coordinator
chown -R postgres:postgres coordinator
#as postgres
initdb -D /var/lib/postgres/coordinator
pg_ctl -D /var/lib/postgres/coordinator -o "-p 9700" start

cd /var/lib/postgres/
mkdir worker1
chown -R postgres:postgres worker1
#as postgres
initdb -D /var/lib/postgres/worker1
pg_ctl -D /var/lib/postgres/worker1 -o "-p 9701" start

cd /var/lib/postgres/
mkdir worker2
chown -R postgres:postgres worker2
#as postgres
initdb -D /var/lib/postgres/worker2
pg_ctl -D /var/lib/postgres/worker2 -o "-p 9702" start

cd /var/lib/postgres/
mkdir worker3
chown -R postgres:postgres worker3
#as postgres
initdb -D /var/lib/postgres/worker3
pg_ctl -D /var/lib/postgres/worker3 -o "-p 9703" start

cd /var/lib/postgres/
mkdir worker4
chown -R postgres:postgres worker4
#as postgres
initdb -D /var/lib/postgres/worker4
pg_ctl -D /var/lib/postgres/worker4 -o "-p 9704" start

#on the coordinator
SELECT * from citus_add_node('localhost', 9701);
SELECT * from citus_add_node('localhost', 9702);
SELECT * from citus_add_node('localhost', 9703);
SELECT * from citus_add_node('localhost', 9704);
SELECT * FROM citus_get_active_worker_nodes();

pg_ctl -D coordinator -o "-p 9700"  start
pg_ctl -D worker1 -o "-p 9701"  start
pg_ctl -D worker2 -o "-p 9702"  start
pg_ctl -D worker3 -o "-p 9703"  start
pg_ctl -D worker4 -o "-p 9704"  start