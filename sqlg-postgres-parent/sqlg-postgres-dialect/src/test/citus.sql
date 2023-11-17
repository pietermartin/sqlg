--as postgres user
#as root
cd /var/lib/postgres/
mkdir coordinator
chown -R postgres:postgres coordinator
mkdir worker1
chown -R postgres:postgres worker1
mkdir worker2
chown -R postgres:postgres worker3
mkdir worker3
chown -R postgres:postgres worker3
mkdir worker4
chown -R postgres:postgres worker4

#as postgres
initdb -D /var/lib/postgres/coordinator
initdb -D /var/lib/postgres/worker1
initdb -D /var/lib/postgres/worker2
initdb -D /var/lib/postgres/worker3
initdb -D /var/lib/postgres/worker4

#as root
echo "shared_preload_libraries = 'citus'" | sudo tee -a /var/lib/postgres/coordinator/postgresql.conf
echo "shared_preload_libraries = 'citus'" | sudo tee -a /var/lib/postgres/worker1/postgresql.conf
echo "shared_preload_libraries = 'citus'" | sudo tee -a /var/lib/postgres/worker2/postgresql.conf
echo "shared_preload_libraries = 'citus'" | sudo tee -a /var/lib/postgres/worker4/postgresql.conf
echo "shared_preload_libraries = 'citus'" | sudo tee -a /var/lib/postgres/worker4/postgresql.conf

#as postgres
pg_ctl -D /var/lib/postgres/coordinator -o "-p 9700" start
pg_ctl -D /var/lib/postgres/worker1 -o "-p 9701" start
pg_ctl -D /var/lib/postgres/worker2 -o "-p 9702" start
pg_ctl -D /var/lib/postgres/worker3 -o "-p 9703" start
pg_ctl -D /var/lib/postgres/worker4 -o "-p 9704" start

#on all nodes
create extension citus;

#on the coordinator
SELECT * from citus_add_node('localhost', 9701);
SELECT * from citus_add_node('localhost', 9702);
SELECT * from citus_add_node('localhost', 9703);
SELECT * from citus_add_node('localhost', 9704);
SELECT * FROM citus_get_active_worker_nodes();