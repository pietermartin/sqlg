package org.umlg.sqlg.test.mod;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.test.BaseTest;

import java.math.BigDecimal;
import java.time.*;
import java.util.Arrays;
import java.util.Collection;

/**
 * Date: 2014/07/13
 * Time: 6:51 PM
 */
@RunWith(Parameterized.class)
public class TestRemoveProperty extends BaseTest {

    @Parameterized.Parameter
    public Object value;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Parameterized.Parameters(name = "{index}: value:{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
//                new Object[][]{
//                        {"haloThere"}
//                });
                new Object[][]{
                        {true},
                        {(byte) 1},
                        {(short) 1},
                        {1},
                        {1L},
                        {1F},
                        {1.111D},
                        {"haloThere"},
                        {LocalDate.now()}, {LocalDateTime.now()}, {LocalTime.now().withNano(0)}, {ZonedDateTime.now()},
                        {Period.of(1, 1, 1)}, {Duration.ofHours(5)}, {objectMapper.createObjectNode()},
                        {new boolean[]{false, true}}, {new Boolean[]{Boolean.TRUE, Boolean.FALSE}}, {new byte[]{(byte) 1, (byte) 2}},
                        {new short[]{(short) 1, (short) 2}}, {new Short[]{(short) 1, (short) 2}}, {new int[]{1, 1}}, {new Integer[]{1, 1}},
                        {new long[]{1L, 2L}}, {new Long[]{1L, 2L}}, {new double[]{2D, 1D}}, {new Double[]{2D, 3D}}, {new BigDecimal[]{BigDecimal.valueOf(2D), BigDecimal.valueOf(3D)}},
                        {new LocalDateTime[]{LocalDateTime.now(), LocalDateTime.now()}},
                        {new LocalDate[]{LocalDate.now(), LocalDate.now()}},
                        {new LocalTime[]{LocalTime.now(), LocalTime.now()}},
                        {new ZonedDateTime[]{ZonedDateTime.now(), ZonedDateTime.now()}},
                        {new Duration[]{Duration.ofHours(1), Duration.ofHours(3)}},
                        {new Period[]{Period.of(1, 1, 1), Period.of(2, 2, 2)}},
                        {new ObjectNode[]{objectMapper.createObjectNode(), objectMapper.createObjectNode()}}
                });
    }

    @Test
    public void shouldAllowRemovalFromVertexWhenAlreadyRemoved() {
        assumeTrueForTest();
        Vertex v = this.sqlgGraph.addVertex("name", this.value);
        this.sqlgGraph.tx().commit();
        Vertex v1 = this.sqlgGraph.vertices(new Object[]{v.id()}).next();
        try {
            Property<String> p = v1.property("name");
            p.remove();
            p.remove();
            v1.property("name").remove();
            v1.property("name").remove();
        } catch (Exception var4) {
            Assert.fail("Removing a vertex property that was already removed should not throw an exception");
        }
    }

    @Test
    public void testRemovePropertyBySettingToNull() {
        assumeTrueForTest();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "prop", this.value);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(this.value, v.property("prop").value());
        v.property("prop", null);
        Assert.assertNull(v.property("prop").value());
    }

    @Test
    public void testRemovePropertyBySettingToNullInTraversal() {
        assumeTrueForTest();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "prop", this.value);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(this.value, v.property("prop").value());
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").property("prop", null);
        traversal.next();
        v = this.sqlgGraph.traversal().V().hasLabel("A").next();
        Assert.assertNull(v.property("prop").value());
    }

    @Test
    public void testRemove() {
        assumeTrueForTest();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "prop", this.value);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(this.value, v.value("prop"));
        v.property("prop").remove();
        this.sqlgGraph.tx().commit();
        Assert.assertNull(this.sqlgGraph.traversal().V(v.id()).next().property("prop").value());
    }

    private void assumeTrueForTest() {
        if (this.value instanceof Float) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        }
        if (this.value instanceof Byte) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteValues());
        }
        if (this.value instanceof boolean[] || this.value instanceof Boolean[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        }
        if (this.value instanceof short[] || this.value instanceof Short[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        }
        if (this.value instanceof int[] || this.value instanceof Integer[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        }
        if (this.value instanceof long[] || this.value instanceof Long[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        }
        if (this.value instanceof float[] || this.value instanceof Float[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        }
        if (this.value instanceof double[] || this.value instanceof Double[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        }
        if (this.value instanceof LocalDateTime[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateTimeArrayValues());
        }
        if (this.value instanceof LocalDate[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateArrayValues());
        }
        if (this.value instanceof LocalTime[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalTimeArrayValues());
        }
        if (this.value instanceof ZonedDateTime[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsZonedDateTimeArrayValues());
        }
        if (this.value instanceof Duration[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDurationArrayValues());
        }
        if (this.value instanceof Period[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPeriodArrayValues());
        }
        if (this.value instanceof JsonNode[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPeriodArrayValues());
        }
    }

}
