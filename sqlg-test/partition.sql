with recursive inh as (

   select i.inhrelid, null::text as parent
   from pg_catalog.pg_inherits i
     join pg_catalog.pg_class cl on i.inhparent = cl.oid
     join pg_catalog.pg_namespace nsp on cl.relnamespace = nsp.oid
--   where nsp.nspname = 'A'          ---<< change table schema here
--     and cl.relname = 'E_ab'   ---<< change table name here

   union all

   select i.inhrelid, (i.inhparent::regclass)::text
   from inh
     join pg_catalog.pg_inherits i on (inh.inhrelid = i.inhparent)
)
select c.relname as partition_name,
        n.nspname as partition_schema,
        pg_get_expr(c.relpartbound, c.oid, true) as partition_expression,
        pg_get_expr(p.partexprs, c.oid, true) as sub_partition,
        parent,
        case p.partstrat
          when 'l' then 'LIST'
          when 'r' then 'RANGE'
        end as sub_partition_strategy
from inh
   join pg_catalog.pg_class c on inh.inhrelid = c.oid
   join pg_catalog.pg_namespace n on c.relnamespace = n.oid
   left join pg_partitioned_table p on p.partrelid = c.oid
order by n.nspname, c.relname



SELECT
--    nmsp_parent.nspname AS parent_schema,
--    parent.relname      AS parent,
--    nmsp_child.nspname  AS child_schema,
--    child.relname       AS child
parent.*
FROM pg_inherits
    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
--WHERE parent.relname='parent_table_name';


SELECT
	c.relpartbound,
	pg_get_expr(c.relpartbound, c.oid, true)
FROM
	pg_namespace n join
    pg_class c on c.relnamespace = n.oid left join
    pg_partitioned_table p on p.partrelid = c.oid
    --pg_partitioned_table p
WHERE
	c.relpartbound IS NOT NULL;




SELECT
	(i.inhparent::regclass)::text as parent,
	(cl.oid::regclass)::text as child,
	pg_get_expr(cl.relpartbound, cl.oid, true),
 	p.partstrat
FROM
	pg_catalog.pg_namespace n join
    pg_catalog.pg_class cl on cl.relnamespace = n.oid left join
    pg_catalog.pg_inherits i on i.inhrelid = cl.oid left join
    pg_catalog.pg_partitioned_table p on p.partrelid = i.inhparent
WHERE
	cl.relpartbound IS NOT NULL


SELECT attrelid::regclass AS tbl
     , attname            AS col
     , atttypid::regtype  AS datatype
     , attnum  -- more attributes?
FROM   pg_attribute
WHERE
    attrelid = '"V_A"'::regclass  -- table name, optionally schema-qualified
AND    attnum > 0
AND    NOT attisdropped
ORDER  BY attnum;


SELECT
	n.nspname,
	(i.inhparent::regclass)::text as parent,
	(cl.oid::regclass)::text as child,
    p.partstrat,
    p.partattrs,
    pg_get_expr(p.partexprs, p.partrelid),
    pg_get_expr(cl.relpartbound, cl.oid, true)
FROM
    sqlg_schema."V_schema" s join
	pg_catalog.pg_namespace n on s.name = n.nspname join
    pg_catalog.pg_class cl on cl.relnamespace = n.oid left join
    pg_catalog.pg_inherits i on i.inhrelid = cl.oid left join
    pg_catalog.pg_partitioned_table p on p.partrelid = cl.relfilenode
WHERE
	cl.relkind <> 'S'

with pg_partitioned_table as (select
    p.partrelid,
    p.partstrat as partitionType,
    p.partnatts,
    string_agg(a.attname, ',' order by a.attnum) "partitionExpression1",
    pg_get_expr(p.partexprs, p.partrelid) "partitionExpression2"
from
(select
	partrelid,
    partstrat,
    partnatts,
    unnest(partattrs) partattrs,
    partexprs
from
	pg_catalog.pg_partitioned_table
) p left join
	pg_catalog.pg_attribute a on partrelid = a.attrelid and p.partattrs = a.attnum
group by
	1,2,3,5
)
SELECT
	n.nspname as schema,
	(i.inhparent::regclass)::text as parent,
	(cl.oid::regclass)::text as child,
    p.partitionType,
    p."partitionExpression1",
    p."partitionExpression2",
    pg_get_expr(cl.relpartbound, cl.oid, true) as "fromToIn"
FROM
    sqlg_schema."V_schema" s join
	pg_catalog.pg_namespace n on s.name = n.nspname join
    pg_catalog.pg_class cl on cl.relnamespace = n.oid left join
    pg_catalog.pg_inherits i on i.inhrelid = cl.oid left join
    pg_partitioned_table p on p.partrelid = cl.relfilenode
WHERE
	cl.relkind <> 'S' and
    (p."partitionExpression1" is not null or p."partitionExpression2" is not null or cl.relpartbound is not null)

