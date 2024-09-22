-- recursive edges
WITH a AS (
	WITH RECURSIVE search_tree("ID", "public.Friend__O", "public.Friend__I", depth, is_cycle, previous, path, epath) AS (
		SELECT e."ID", e."public.Friend__O", e."public.Friend__I", 1, false, ARRAY[e."public.Friend__O"], ARRAY[e."public.Friend__O", e."public.Friend__I"], ARRAY[e."ID"]
		FROM "E_of" e
		WHERE e."ID" = 1
		UNION ALL
		SELECT e."ID", e."public.Friend__O", e."public.Friend__I", st.depth + 1, e."public.Friend__I" = ANY(path), path, path || e."public.Friend__I", epath || e."ID"
		FROM "E_of" e, search_tree st
		WHERE st."public.Friend__I" = e."public.Friend__O" AND NOT is_cycle
	)
	SELECT * FROM search_tree WHERE NOT is_cycle
), b AS (
	SELECT 'vertex' as "type", a.path FROM a
	WHERE a.path NOT IN (SELECT previous from a)
	UNION ALL
	SELECT 'edge' as "type", a.epath FROM a
	WHERE a.path NOT IN (SELECT previous from a)
), c AS (
    SELECT * FROM b JOIN UNNEST(b.path) WITH ORDINALITY AS c(element_id, ordinal) ON b."type" = 'vertex' WHERE b."type" = 'vertex'
    UNION ALL
	SELECT * FROM b JOIN UNNEST(b.path) WITH ORDINALITY AS c(element_id, ordinal) ON b."type" = 'edge' WHERE b."type" = 'edge'
), d AS (
    SELECT type, ordinal, "ID", _v.name FROM c JOIN "V_Friend" AS _v on c.element_id = _v."ID" WHERE c.type = 'vertex'
	UNION ALL
    SELECT type, ordinal, "ID", null FROM c JOIN "E_of" AS _e on c.element_id = _e."ID" WHERE c.type = 'edge'
)
SELECT * from d
ORDER BY ordinal