WITH RECURSIVE search_tree("ID", "public.Friend__O", "public.Friend__I", depth, is_cycle, path, direction, leaf) AS (
    SELECT e."ID", e."public.Friend__O", e."public.Friend__I", 1, false,
           CASE
               WHEN "public.Friend__O" = 1 THEN ARRAY[e."public.Friend__O", e."public.Friend__I"]
               WHEN "public.Friend__I" = 1 THEN ARRAY[e."public.Friend__I", e."public.Friend__O"]
               END,
           CASE
               WHEN "public.Friend__O" = 1 THEN 'OUT'
               WHEN "public.Friend__I" = 1 THEN 'IN'
               END,
           CASE
               WHEN "public.Friend__O" = 1	THEN NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__I" = ee."public.Friend__O")
               WHEN "public.Friend__I" = 1	THEN NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__O" = ee."public.Friend__I")
               END
    FROM "E_of" e
    WHERE "public.Friend__O" = 1 or "public.Friend__I" = 1
    UNION ALL
    SELECT e."ID", e."public.Friend__O", e."public.Friend__I", st.depth + 1,
           CASE
               WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN e."public.Friend__I" = ANY(path)
               WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN e."public.Friend__O" = ANY(path)
               WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN e."public.Friend__O" = ANY(path)
               WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN e."public.Friend__I" = ANY(path)
               END,
           CASE
               WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN path || e."public.Friend__I"
               WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN path || e."public.Friend__O"
               WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN path || e."public.Friend__O"
               WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN path || e."public.Friend__I"
               END,
           CASE
               WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN 'OUT'
               WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN 'IN'
               WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN 'IN'
               WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN 'OUT'
               END,
           CASE
               WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__I" = ee."public.Friend__O")
               WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__O" = ee."public.Friend__I")
               WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__O" = ee."public.Friend__I")
               WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__I" = ee."public.Friend__O")
               END
    FROM "E_of" e, search_tree st
    WHERE
        (
            (st.direction = 'OUT' AND (st."public.Friend__I" = e."public.Friend__O" OR st."public.Friend__I" = e."public.Friend__I"))
                OR
            (st.direction = 'IN' AND (st."public.Friend__O" = e."public.Friend__I" OR st."public.Friend__O" = e."public.Friend__O"))
            )
      AND NOT is_cycle
)
SELECT * FROM search_tree
WHERE NOT is_cycle
ORDER BY path;

--OUT
WITH RECURSIVE search_tree("ID", "public.Friend__O", "public.Friend__I", depth, is_cycle, path, leaf) AS (
    SELECT e."ID", e."public.Friend__O", e."public.Friend__I", 1, false, ARRAY[e."public.Friend__O", e."public.Friend__I"],
           NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__I" = ee."public.Friend__O")
    FROM "E_of" e
    WHERE "public.Friend__O" = 1
    UNION ALL
    SELECT e."ID", e."public.Friend__O", e."public.Friend__I", st.depth + 1, e."public.Friend__I" = ANY(path), path || e."public.Friend__I",
           NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__I" = ee."public.Friend__O")
    FROM "E_of" e, search_tree st
    WHERE st."public.Friend__I" = e."public.Friend__O" AND NOT is_cycle
)
SELECT * FROM search_tree
WHERE NOT is_cycle
ORDER BY path;

--IN
WITH RECURSIVE search_tree("ID", "public.Friend__I", "public.Friend__O", depth, is_cycle, path, leaf) AS (
    SELECT e."ID", e."public.Friend__I", e."public.Friend__O", 1, false, ARRAY[e."public.Friend__I", e."public.Friend__O"],
           NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__O" = ee."public.Friend__I")
    FROM "E_of" e
    WHERE "public.Friend__I" = 3
    UNION ALL
    SELECT e."ID", e."public.Friend__I", e."public.Friend__O", st.depth + 1, e."public.Friend__O" = ANY(path), path || e."public.Friend__O",
           NOT EXISTS(SELECT NULL FROM "E_of" ee WHERE e."public.Friend__O" = ee."public.Friend__I")
    FROM "E_of" e, search_tree st
    WHERE st."public.Friend__O" = e."public.Friend__I" AND NOT is_cycle
)
SELECT * FROM search_tree WHERE leaf;

--pivot left join
SELECT * FROM search_tree LEFT JOIN UNNEST(PATH) WITH ORDINALITY AS b(vertex_id, ordinal) ON true
WHERE leaf ORDER BY "ID", vertex_id, ordinal;

SELECT DISTINCT ON (test) test, path FROM (
  WITH RECURSIVE search_tree("ID", "public.Friend__O", "public.Friend__I", depth, is_cycle, path, test) AS (
      SELECT e."ID", e."public.Friend__O", e."public.Friend__I", 1, false, ARRAY[e."public.Friend__O", e."public.Friend__I"], gen_random_uuid()
      FROM "E_of" e
      WHERE "public.Friend__O" = 1
      UNION ALL
      SELECT e."ID", e."public.Friend__O", e."public.Friend__I", st.depth + 1, e."public.Friend__I" = ANY(path), path || e."public.Friend__I", st.test
      FROM "E_of" e, search_tree st
      WHERE st."public.Friend__I" = e."public.Friend__O" AND NOT is_cycle
  )
  SELECT * FROM search_tree
  WHERE NOT is_cycle
  )
ORDER BY test, path DESC