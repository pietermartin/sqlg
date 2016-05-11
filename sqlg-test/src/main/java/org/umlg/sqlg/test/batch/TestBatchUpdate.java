package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;

/**
 * Date: 2015/12/14
 * Time: 8:56 PM
 */
public class TestBatchUpdate extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testUpdateWithoutDots() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 4; i++) {
            this.sqlgGraph.addVertex(T.label, "GTRX", "test1", "a1", "test2", "a2", "cm_uid", "cm_uid_" + i);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 1;
        GraphTraversalSource gts = this.sqlgGraph.traversal();
        Vertex cmUid0 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next();
        cmUid0.property("test1", "b1");

        Vertex cmUid1 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next();
        cmUid1.property("test2", "b1");

        Vertex cmUid2 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next();
        cmUid2.property("test1", "b1");

        Vertex cmUid3 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next();
        cmUid3.property("test2", "b1");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test1"));
        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test2"));

        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test1"));
        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test2"));

        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test1"));
        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test2"));

        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test1"));
        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test2"));
    }

    @Test
    public void testUpdateWithDots() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 4; i++) {
            this.sqlgGraph.addVertex(T.label, "GTRX", "test1", "a1", "test.2", "a2", "cm_uid", "cm_uid_" + i);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 1;
        GraphTraversalSource gts = this.sqlgGraph.traversal();
        Vertex cmUid0 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next();
        cmUid0.property("test1", "b1");

        Vertex cmUid1 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next();
        cmUid1.property("test.2", "b1");

        Vertex cmUid2 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next();
        cmUid2.property("test1", "b1");

        Vertex cmUid3 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next();
        cmUid3.property("test.2", "b1");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test1"));
        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test.2"));

        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test1"));
        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test.2"));

        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test1"));
        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test.2"));

        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test1"));
        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test.2"));
    }

    @Test
    public void testUpdateArrays() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "names", new String[]{"A", "B"},
                "integers", new int[]{1, 2},
                "booleans", new boolean[]{true, true},
                "doubles", new double[]{1d, 2d},
                "longs", new long[]{1l, 2l},
                "floats", new float[]{1f, 2f},
                "shorts", new short[]{1, 2},
                "bytes", new byte[]{1, 2}
        );
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").next().property("names").isPresent());
        Assert.assertArrayEquals(new String[]{"A", "B"}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<String[]>value("names"));
        Assert.assertArrayEquals(new byte[]{1, 2}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
        this.sqlgGraph.tx().normalBatchModeOn();
        v.property("names", new String[]{"C", "D"});
        v.property("integers", new int[]{3, 4});
        v.property("booleans", new boolean[]{false, false});
        v.property("doubles", new double[]{3d, 4d});
        v.property("longs", new long[]{3l, 4l});
        v.property("floats", new float[]{3f, 4f});
        v.property("shorts", new short[]{3, 4});
        v.property("bytes", new byte[]{3, 4});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new String[]{"C", "D"}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<String[]>value("names"));
        Assert.assertArrayEquals(new int[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<int[]>value("integers"));
        Assert.assertArrayEquals(new boolean[]{false, false}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<boolean[]>value("booleans"));
        Assert.assertArrayEquals(new double[]{3d, 4d}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<double[]>value("doubles"), 0d);
        Assert.assertArrayEquals(new long[]{3l, 4l}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<long[]>value("longs"));
        Assert.assertArrayEquals(new float[]{3f, 4f}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<float[]>value("floats"), 0f);
        Assert.assertArrayEquals(new short[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<short[]>value("shorts"));
        Assert.assertArrayEquals(new byte[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
    }

    @Test
    public void testLocalDateTime() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDateTime", LocalDateTime.of(1974, 6, 6, 6, 6));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime localDateTime = LocalDateTime.now();
        a1.property("localDateTime", localDateTime);
        this.sqlgGraph.tx().commit();
        a1 = this.sqlgGraph.traversal().V(a1).next();
        Assert.assertEquals(localDateTime, a1.value("localDateTime"));
    }

    @Test
    public void testLocalDate() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDateTime", LocalDate.of(1974, 6, 6));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate localDate = LocalDate.now();
        a1.property("localDate", localDate);
        this.sqlgGraph.tx().commit();
        a1 = this.sqlgGraph.traversal().V(a1).next();
        Assert.assertEquals(localDate, a1.value("localDate"));
    }

    @Test
    public void testLocalTime() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localTime", LocalTime.of(4, 6, 6), "localDate", LocalDate.of(1974,1,1));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime localTime = LocalTime.now();
        LocalDate localDate = LocalDate.now();
        a1.property("localTime", localTime);
        a1.property("localDate", localDate);
        this.sqlgGraph.tx().commit();
        a1 = this.sqlgGraph.traversal().V(a1).next();
        Assert.assertEquals(localTime.toSecondOfDay(), a1.<LocalTime>value("localTime").toSecondOfDay());
    }

    @Test
    public void testZonedDateTime() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTime", ZonedDateTime.of(LocalDate.of(4, 6, 6), LocalTime.of(1, 1), ZoneId.systemDefault()));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        a1.property("zonedDateTime", zonedDateTime);
        this.sqlgGraph.tx().commit();
        a1 = this.sqlgGraph.traversal().V(a1).next();
        Assert.assertEquals(zonedDateTime, a1.value("zonedDateTime"));
    }

    @Test
    public void testDuration() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "duration", Duration.ofHours(5));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Duration duration = Duration.ofHours(10);
        a1.property("duration", duration);
        this.sqlgGraph.tx().commit();
        a1 = this.sqlgGraph.traversal().V(a1).next();
        Assert.assertEquals(duration, a1.value("duration"));
    }

    @Test
    public void testPeriod() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "duration", Period.of(5, 5, 5));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Period period = Period.of(10, 1, 1);
        a1.property("period", period);
        this.sqlgGraph.tx().commit();
        a1 = this.sqlgGraph.traversal().V(a1).next();
        Assert.assertEquals(period, a1.value("period"));
    }
}
