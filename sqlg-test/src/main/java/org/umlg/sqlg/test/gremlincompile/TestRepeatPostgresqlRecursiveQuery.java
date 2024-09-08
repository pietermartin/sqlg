package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class TestRepeatPostgresqlRecursiveQuery extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRepeatPostgresqlRecursiveQuery.class);

    //    @Test
    public void testFriendOfFriendOut() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                )
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        StopWatch stopWatch = StopWatch.createStarted();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d");
        Vertex e = sqlgGraph.addVertex(T.label, "Friend", "name", "e");
        Vertex f = sqlgGraph.addVertex(T.label, "Friend", "name", "f");
        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", d);
        c.addEdge("of", e);
        a.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        Vertex first = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "a").tryNext().orElseThrow();
        List<Path> paths = this.sqlgGraph.traversal().V(first)
                .repeat(__.out("of").simplePath()).until(__.not(__.out("of").simplePath()))
                .path()
                .toList();
        stopWatch.stop();
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(e)));
        LOGGER.info("repeat query time: {}", stopWatch);

        stopWatch.reset();
        stopWatch.start();
        ListOrderedSet<RepeatRow> result = executeSqlForDirectionOUT(((RecordId) first.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 6L})));
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L, 4L})));
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L, 5L})));

        stopWatch.reset();
        stopWatch.start();
        first = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "d").tryNext().orElseThrow();
        paths = this.sqlgGraph.traversal().V(first)
                .repeat(__.in("of").simplePath()).until(__.not(__.in("of").simplePath()))
                .path()
                .toList();
        stopWatch.stop();
        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(4, paths.get(0).size());
        LOGGER.info("repeat query time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();
        result = executeSqlForDirectionIN(((RecordId) first.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{4L, 3L, 2L, 1L})));

    }

//    @Test
    public void testFriendOfFriendCycles() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                )
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        StopWatch stopWatch = StopWatch.createStarted();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d");
        Vertex e = sqlgGraph.addVertex(T.label, "Friend", "name", "e");
        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", a);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        Vertex first = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "a").tryNext().orElseThrow();
        List<Path> paths = this.sqlgGraph.traversal().V(first)
                .repeat(__.out("of").simplePath()).until(__.not(__.out("of").simplePath()))
                .path()
                .toList();
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c)));

        stopWatch.reset();
        stopWatch.start();
        ListOrderedSet<RepeatRow> result = executeSqlForDirectionOUT(((RecordId) first.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L})));

        stopWatch.reset();
        stopWatch.start();
        first = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "a").tryNext().orElseThrow();
        paths = this.sqlgGraph.traversal().V(first)
                .repeat(__.in("of").simplePath()).until(__.not(__.in("of").simplePath()))
                .path()
                .toList();
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(c) && p.get(2).equals(b)));

        stopWatch.reset();
        stopWatch.start();
        result = executeSqlForDirectionIN(((RecordId)first.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 3L, 2L})));

    }

//    @Test
    public void testFriendOfFriendBOTH() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                )
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d");

        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", b);
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = StopWatch.createStarted();
        List<Path> paths = this.sqlgGraph.traversal().V(a)
                .repeat(__.both("of").simplePath()).until(__.not(__.both("of").simplePath()))
                .path()
                .toList();
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c)));
        Assert.assertEquals(paths.get(0), paths.get(1));
        stopWatch.reset();
        stopWatch.start();
        ListOrderedSet<RepeatRow> result = executeSqlForDirectionBOTH(((RecordId)a.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L})));
        Assert.assertArrayEquals(result.get(0).path, result.get(1).path);
    }

    @Test
    public void testFriendOfFriendBOTHComplicated() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                )
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c");
        Vertex aa = sqlgGraph.addVertex(T.label, "Friend", "name", "aa");
        Vertex bb = sqlgGraph.addVertex(T.label, "Friend", "name", "b");

        a.addEdge("of", b);
        b.addEdge("of", c);
        aa.addEdge("of", bb);
        aa.addEdge("of", b);
        bb.addEdge("of", c);
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = StopWatch.createStarted();
        List<Path> paths = this.sqlgGraph.traversal().V(a)
                .repeat(__.both("of").simplePath()).until(__.not(__.both("of").simplePath()))
                .path()
                .toList();
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(bb) && p.get(4).equals(aa)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(aa) && p.get(3).equals(bb) && p.get(4).equals(c)));
        stopWatch.reset();
        stopWatch.start();
        ListOrderedSet<RepeatRow> result = executeSqlForDirectionBOTH(((RecordId)a.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L, 5L, 4L})));
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 4L, 5L, 3L})));
    }

    private ListOrderedSet<RepeatRow> executeSqlForDirectionOUT(Long startNode) {
        ListOrderedSet<RepeatRow> result = new ListOrderedSet<>();
        Connection connection = this.sqlgGraph.tx().getConnection();
        String sql = """
                --OUT
                WITH a AS (
                WITH RECURSIVE search_tree("ID", "public.Friend__O", "public.Friend__I", depth, is_cycle, previous, path) AS (
                    SELECT e."ID", e."public.Friend__O", e."public.Friend__I", 1, false, ARRAY[e."public.Friend__O"], ARRAY[e."public.Friend__O", e."public.Friend__I"]
                    FROM "E_of" e
                    WHERE "public.Friend__O" = {x}
                    UNION ALL
                    SELECT e."ID", e."public.Friend__O", e."public.Friend__I", st.depth + 1, e."public.Friend__I" = ANY(path), path, path || e."public.Friend__I"
                    FROM "E_of" e, search_tree st
                    WHERE st."public.Friend__I" = e."public.Friend__O" AND NOT is_cycle
                )
                SELECT * FROM search_tree
                WHERE NOT is_cycle
                )
                SELECT a.path from a
                WHERE a.path NOT IN (SELECT previous from a)
                """;
        sql = sql.replace("{x}", startNode.toString());
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                Array path = rs.getArray(1);
                result.add(new RepeatRow(((Long[]) path.getArray())));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private ListOrderedSet<RepeatRow> executeSqlForDirectionIN(Long startNode) {
        ListOrderedSet<RepeatRow> result = new ListOrderedSet<>();
        Connection connection = this.sqlgGraph.tx().getConnection();
        String sql = """
                WITH a AS (
                WITH RECURSIVE search_tree("ID", "public.Friend__I", "public.Friend__O", depth, is_cycle, previous, path) AS (
                    SELECT e."ID", e."public.Friend__I", e."public.Friend__O", 1, false, ARRAY[e."public.Friend__I"], ARRAY[e."public.Friend__I", e."public.Friend__O"]
                    FROM "E_of" e
                    WHERE "public.Friend__I" = {x} 
                    UNION ALL
                    SELECT e."ID", e."public.Friend__I", e."public.Friend__O", st.depth + 1, e."public.Friend__O" = ANY(path), path, path || e."public.Friend__O"
                    FROM "E_of" e, search_tree st
                    WHERE st."public.Friend__O" = e."public.Friend__I" AND NOT is_cycle
                )
                SELECT * FROM search_tree\s
                WHERE NOT is_cycle
                )
                SELECT a.path from a
                WHERE a.path NOT IN (SELECT previous from a);
                """;
        sql = sql.replace("{x}", startNode.toString());
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                Array path = rs.getArray(1);
                result.add(new RepeatRow(((Long[]) path.getArray())));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private ListOrderedSet<RepeatRow> executeSqlForDirectionBOTH(Long startNode) {
        ListOrderedSet<RepeatRow> result = new ListOrderedSet<>();
        Connection connection = this.sqlgGraph.tx().getConnection();
        String sql = """
                WITH a as (
                    WITH RECURSIVE search_tree("ID", "public.Friend__O", "public.Friend__I", depth, is_cycle, previous, path, direction) AS (
                        SELECT e."ID", e."public.Friend__O", e."public.Friend__I", 1, false,
                               CASE
                                   WHEN "public.Friend__O" = 1 THEN ARRAY[e."public.Friend__O"]
                                   WHEN "public.Friend__I" = 1 THEN ARRAY[e."public.Friend__I"]
                                   END,
                               CASE
                                   WHEN "public.Friend__O" = 1 THEN ARRAY[e."public.Friend__O", e."public.Friend__I"]
                                   WHEN "public.Friend__I" = 1 THEN ARRAY[e."public.Friend__I", e."public.Friend__O"]
                                   END,
                               CASE
                                   WHEN "public.Friend__O" = 1 THEN 'OUT'
                                   WHEN "public.Friend__I" = 1 THEN 'IN'
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
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN path
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN path
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN path
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN path
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
                )
                SELECT a.path from a
                WHERE a.path NOT IN (SELECT previous from a);
                """;
        sql = sql.replace("{x}", startNode.toString());
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                Array path = rs.getArray(1);
                result.add(new RepeatRow(((Long[]) path.getArray())));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private record RepeatRow(Long[] path) {

    }
}
