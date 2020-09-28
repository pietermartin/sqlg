package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;

import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.temporal.ChronoUnit;

/**
 * Date: 2014/07/13
 * Time: 7:48 PM
 */
@SuppressWarnings("UnnecessaryBoxing")
public class TestSetProperty extends BaseTest {

    @Test
    public void testSetByteProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("byte", Byte.valueOf((byte) 1));
        this.sqlgGraph.tx().commit();
        assertProperty(marko, "byte", (byte) 1);
    }

    @Test
    public void testSetBooleanArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("bools", new Boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Boolean[]{true, false}, (Boolean[]) marko.property("bools").value());
    }


    @Test
    public void testSetByteArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("bytes", new Byte[]{(byte) 1, (byte) 2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Byte[]{(byte) 1, (byte) 2}, (Byte[]) marko.property("bytes").value());

        marko.property("bytesText", "I pack some weirdness:'\",:/?".getBytes(StandardCharsets.UTF_8));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("I pack some weirdness:'\",:/?", new String((byte[]) marko.property("bytesText").value(), StandardCharsets.UTF_8));

    }

    @Test
    public void testSetDoubleArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("doubles", new Double[]{1.0, 2.2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Double[]{1.0, 2.2}, (Double[]) marko.property("doubles").value());
    }

    @Test
    public void testSetFloatArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("floats", new Float[]{1.0f, 2.2f});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Float[]{1.0f, 2.2f}, (Float[]) marko.property("floats").value());
    }

    @Test
    public void testSetIntegerArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("integers", new Integer[]{1, 2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Integer[]{1, 2}, (Integer[]) marko.property("integers").value());
    }

    @Test
    public void testSetLongArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("longs", new Long[]{1L, 2L});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Long[]{1L, 2L}, (Long[]) marko.property("longs").value());
    }

    @Test
    public void testSetShortArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("shorts", new Short[]{1, 2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Short[]{1, 2}, (Short[]) marko.property("shorts").value());
    }

    @Test
    public void testSetBooleanPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("bools", new boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new boolean[]{true, false}, (boolean[]) marko.property("bools").value());
    }

    @Test
    public void testSetBytePrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("bytes", new byte[]{(byte) 1, (byte) 2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new byte[]{(byte) 1, (byte) 2}, (byte[]) marko.property("bytes").value());

        marko.property("bytesText", "I pack some weirdness:'\",:/?".getBytes(StandardCharsets.UTF_8));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("I pack some weirdness:'\",:/?", new String((byte[]) marko.property("bytesText").value(), StandardCharsets.UTF_8));

    }

    @Test
    public void testSetDoublePrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("doubles", new double[]{1.0, 2.2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new double[]{1.0, 2.2}, (double[]) marko.property("doubles").value(), 0.00001);
    }

    @Test
    public void testSetFloatPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("floats", new float[]{1.0f, 2.2f});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new float[]{1.0f, 2.2f}, (float[]) marko.property("floats").value(), 0.00001f);
    }

    @Test
    public void testSetIntegerPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("integers", new int[]{1, 2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new int[]{1, 2}, (int[]) marko.property("integers").value());
    }

    @Test
    public void testSetLongPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("longs", new long[]{1L, 2L});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new long[]{1L, 2L}, (long[]) marko.property("longs").value());
    }

    @Test
    public void testSetShortPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("shorts", new short[]{1, 2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new short[]{1, 2}, (short[]) marko.property("shorts").value());
    }


    @Test
    public void testSetStringArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStringArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("strings", new String[]{"a", "b"});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new String[]{"a", "b"}, (String[]) marko.property("strings").value());
    }


    @Test
    public void testSetPrimitiveProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        v.property("age2", (short) 1);
        v.property("age3", 1);
        v.property("age4", 1L);
        v.property("age5", 1f);
        v.property("age6", 1d);
        v.property("ok", true);

        this.sqlgGraph.tx().commit();
        assertProperty(v, "age2", (short) 1);
        assertProperty(v, "age3", 1);
        assertProperty(v, "age4", 1L);
        assertProperty(v, "age5", 1f);
        assertProperty(v, "age6", 1d);
        assertProperty(v, "ok", true);

    }

    @Test
    public void testSetObjectProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        v.property("age2", Short.valueOf((short) 1));
        v.property("age3", Integer.valueOf(1));
        v.property("age4", Long.valueOf(1L));
        v.property("age5", Float.valueOf(1f));
        v.property("age6", Double.valueOf(1d));
        v.property("ok", Boolean.TRUE);

        this.sqlgGraph.tx().commit();
        assertProperty(v, "age2", Short.valueOf((short) 1));
        assertProperty(v, "age3", Integer.valueOf(1));
        assertProperty(v, "age4", Long.valueOf(1L));
        assertProperty(v, "age5", Float.valueOf(1f));
        assertProperty(v, "age6", Double.valueOf(1d));
        assertProperty(v, "ok", Boolean.TRUE);

    }

    @Test
    public void testSetProperty() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("surname", "xxxx");
        this.sqlgGraph.tx().commit();
        assertProperty(marko, "surname", "xxxx");
    }

    @Test
    public void testPropertyManyTimes() {
        Vertex v = this.sqlgGraph.addVertex("age", 1, "name", "marko", "name", "john");
        this.sqlgGraph.tx().commit();
        assertProperty(v, "name", "john");
    }

    @Test
    public void testSetPropertyManyTimes() {
        Vertex v = this.sqlgGraph.addVertex("age", 1, "name", "marko");
        v.property("name", "tony");
        v.property("name", "john");
        this.sqlgGraph.tx().commit();
        assertProperty(v, "name", "john");
    }

    @Test
    public void testFloat() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "age", 1f);
        this.sqlgGraph.tx().commit();
        assertProperty(v, "age", 1f);
    }

    @Test
    public void testPrimitiveProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", (short) 1,
                "age3", 1,
                "age4", 1L,
                "age5", 1f,
                "age6", 1d,
                "ok", true
        );
        this.sqlgGraph.tx().commit();
        assertProperty(v, "age2", (short) 1);
        assertProperty(v, "age3", 1);
        assertProperty(v, "age4", 1L);
        assertProperty(v, "age5", 1f);
        assertProperty(v, "age6", 1d);
        assertProperty(v, "ok", true);
    }

    @Test
    public void testPrimitivePropertiesNoFloat() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", (short) 1,
                "age3", 1,
                "age4", 1L,
                "age6", 1d,
                "ok", true
        );
        this.sqlgGraph.tx().commit();
        assertProperty(v, "age2", (short) 1);
        assertProperty(v, "age3", 1);
        assertProperty(v, "age4", 1L);
        assertProperty(v, "age6", 1d);
        assertProperty(v, "ok", true);
    }

    @Test
    public void testObjectProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", Short.valueOf((short) 1),
                "age3", Integer.valueOf(1),
                "age4", Long.valueOf(1L),
                "age5", Float.valueOf(1f),
                "age6", Double.valueOf(1d),
                "ok", Boolean.TRUE
        );
        this.sqlgGraph.tx().commit();
        assertProperty(v, "age2", Short.valueOf((short) 1));
        assertProperty(v, "age3", Integer.valueOf(1));
        assertProperty(v, "age4", Long.valueOf(1L));
        assertProperty(v, "age5", Float.valueOf(1f));
        assertProperty(v, "age6", Double.valueOf(1d));
        assertProperty(v, "ok", Boolean.TRUE);
    }

    @Test
    public void testObjectPropertiesNoFloat() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", Short.valueOf((short) 1),
                "age3", Integer.valueOf(1),
                "age4", Long.valueOf(1L),
                "age6", Double.valueOf(1d),
                "ok", Boolean.TRUE
        );
        this.sqlgGraph.tx().commit();
        assertProperty(v, "age2", Short.valueOf((short) 1));
        assertProperty(v, "age3", Integer.valueOf(1));
        assertProperty(v, "age4", Long.valueOf(1L));
        assertProperty(v, "age6", Double.valueOf(1d));
        assertProperty(v, "ok", Boolean.TRUE);
    }

    @Test
    public void testDateTimeProperties() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        LocalDateTime ldt = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        v.property("ldt", ldt);
        v.property("ld", ldt.toLocalDate());
        LocalTime lt = ldt.toLocalTime().truncatedTo(ChronoUnit.SECONDS);
        v.property("lt", lt);

        ZonedDateTime zdt = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("Africa/Johannesburg")).truncatedTo(ChronoUnit.MILLIS);
        ZonedDateTime zdt1Fixed = ZonedDateTime.of(zdt.toLocalDateTime(), ZoneId.of("Africa/Johannesburg")).truncatedTo(ChronoUnit.MILLIS);
        v.property("zdt", zdt);

        ZonedDateTime zdt2 = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("+02:00")).truncatedTo(ChronoUnit.MILLIS);
        ZonedDateTime zdt2Fixed = ZonedDateTime.of(zdt2.toLocalDateTime(), ZoneId.of("GMT+02:00"));
        v.property("zdt2", zdt2);


        Period p = Period.ofDays(3);
        v.property("p", p);

        Duration d = Duration.ofHours(12);
        v.property("d", d);

        Vertex vJ = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        vJ.addEdge("knows", v);

        this.sqlgGraph.tx().commit();
        assertProperty(v, "ldt", ldt);
        assertProperty(v, "ld", ldt.toLocalDate());
        assertProperty(v, "lt", lt);
        assertProperty(v, "zdt", zdt1Fixed);
        assertProperty(v, "zdt2", zdt2Fixed);
        assertProperty(v, "p", p);
        assertProperty(v, "d", d);
        Vertex vJ2 = sqlgGraph.vertices(vJ.id()).next();
        Vertex v2 = vJ2.edges(Direction.OUT).next().inVertex();
        assertProperty(v2, "ldt", ldt);
        assertProperty(v2, "ld", ldt.toLocalDate());
        assertProperty(v2, "lt", lt);
        assertProperty(v2, "zdt", zdt);
        assertProperty(v2, "zdt2", zdt2Fixed);
        assertProperty(v2, "p", p);
        assertProperty(v2, "d", d);

        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            v2 = sqlgGraph1.vertices(v.id()).next();
            assertProperty(sqlgGraph1, v2, "ldt", ldt);
            assertProperty(sqlgGraph1, v2, "ld", ldt.toLocalDate());
            assertProperty(sqlgGraph1, v2, "lt", lt);
            assertProperty(sqlgGraph1, v2, "zdt", zdt);
            assertProperty(sqlgGraph1, v2, "zdt2", zdt2Fixed);
            assertProperty(sqlgGraph1, v2, "p", p);
            assertProperty(sqlgGraph1, v2, "d", d);
        }
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            v2 = sqlgGraph1.traversal().V().hasLabel("Person").next();
            assertProperty(sqlgGraph1, v2, "ldt", ldt);
            assertProperty(sqlgGraph1, v2, "ld", ldt.toLocalDate());
            assertProperty(sqlgGraph1, v2, "lt", lt);
            assertProperty(sqlgGraph1, v2, "zdt", zdt);
            assertProperty(sqlgGraph1, v2, "zdt2", zdt2Fixed);
            assertProperty(sqlgGraph1, v2, "p", p);
            assertProperty(sqlgGraph1, v2, "d", d);

        }
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            vJ2 = sqlgGraph1.vertices(vJ.id()).next();
            v2 = vJ2.edges(Direction.OUT).next().inVertex();
            assertProperty(sqlgGraph1, v2, "ldt", ldt);
            assertProperty(sqlgGraph1, v2, "ld", ldt.toLocalDate());
            assertProperty(sqlgGraph1, v2, "lt", lt);
            assertProperty(sqlgGraph1, v2, "zdt", zdt);
            assertProperty(sqlgGraph1, v2, "zdt2", zdt2Fixed);
            assertProperty(sqlgGraph1, v2, "p", p);
            assertProperty(sqlgGraph1, v2, "d", d);

        }
    }

    @Test
    public void testEdgeDateTimeProperties() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");

        Vertex vJ = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Edge e1 = vJ.addEdge("knows", v);

        LocalDateTime ldt = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        e1.property("ldt", ldt);
        e1.property("ld", ldt.toLocalDate());
        LocalTime lt = ldt.toLocalTime().truncatedTo(ChronoUnit.SECONDS);
        e1.property("lt", lt);

        ZonedDateTime zdt = ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        e1.property("zdt", zdt);

        Period p = Period.ofDays(3);
        e1.property("p", p);

        Duration d = Duration.ofHours(12);
        e1.property("d", d);

        this.sqlgGraph.tx().commit();
        assertProperty(e1, "ldt", ldt);
        assertProperty(e1, "ld", ldt.toLocalDate());
        assertProperty(e1, "lt", lt);
        assertProperty(e1, "zdt", zdt);
        assertProperty(e1, "p", p);
        assertProperty(e1, "d", d);
        Vertex vJ2 = sqlgGraph.vertices(vJ.id()).next();
        Edge e2 = vJ2.edges(Direction.OUT).next();
        assertProperty(e2, "ldt", ldt);
        assertProperty(e2, "ld", ldt.toLocalDate());
        assertProperty(e2, "lt", lt);
        assertProperty(e2, "zdt", zdt);
        assertProperty(e2, "p", p);
        assertProperty(e2, "d", d);
    }

    @Test
    public void testDateTimeArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateTimeArrayValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        LocalDateTime ldt = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        v.property("ldt", new LocalDateTime[]{ldt});
        v.property("ld", new LocalDate[]{ldt.toLocalDate()});
        LocalTime lt = ldt.toLocalTime().truncatedTo(ChronoUnit.SECONDS);
        v.property("lt", new LocalTime[]{lt});

        ZonedDateTime zdt = ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        v.property("zdt", new ZonedDateTime[]{zdt});

        ZonedDateTime zdt2 = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("+02:00")).truncatedTo(ChronoUnit.MILLIS);
        v.property("zdt2", new ZonedDateTime[]{zdt2});

        Period p = Period.ofDays(3);
        v.property("p", new Period[]{p});

        Duration d = Duration.ofHours(12);
        v.property("d", new Duration[]{d});

        this.sqlgGraph.tx().commit();
        assertObjectArrayProperty(v, "ldt", ldt);
        assertObjectArrayProperty(v, "ld", ldt.toLocalDate());
        assertObjectArrayProperty(v, "lt", lt);
        assertObjectArrayProperty(v, "zdt", zdt);
        assertObjectArrayProperty(v, "zdt2", zdt2);
        assertObjectArrayProperty(v, "p", p);
        assertObjectArrayProperty(v, "d", d);
    }

    private <TP> void assertProperty(Vertex v, String property, TP expected) {
        assertProperty(this.sqlgGraph, v, property, expected);
    }

    private static <TP> void assertProperty(SqlgGraph g, Vertex v, String property, TP expected) {
//        Assert.assertEquals(expected, v.property(property).value());
        Assert.assertEquals(expected, g.traversal().V(v).values(property).next());
    }

    private <TP> void assertProperty(Edge e, String property, TP expected) {
        assertProperty(this.sqlgGraph, e, property, expected);
    }

    private static <TP> void assertProperty(SqlgGraph g, Edge e, String property, TP expected) {
        Assert.assertEquals(expected, e.property(property).value());
        Assert.assertEquals(expected, g.traversal().E(e).values(property).next());
    }

    private void assertObjectArrayProperty(Vertex v, String property, Object... expected) {
        Assert.assertArrayEquals(expected, (Object[]) v.property(property).value());
        Assert.assertArrayEquals(expected, (Object[]) this.sqlgGraph.traversal().V(v).values(property).next());
    }
}
