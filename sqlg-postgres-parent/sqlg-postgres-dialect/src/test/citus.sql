--as postgres user
pg_ctl -D coordinator -o "-p 9700"  start
pg_ctl -D worker1 -o "-p 9701"  start
pg_ctl -D worker2 -o "-p 9702"  start
pg_ctl -D worker3 -o "-p 9703"  start
pg_ctl -D worker4 -o "-p 9704"  start