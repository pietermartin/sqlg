package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/07/25
 */
@RunWith(Parameterized.class)
public class TestBatchNormalNullUpdate extends BaseTest {

    @Parameterized.Parameter
    public Object value;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
//        return Arrays.asList(new Object[][]{{ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS)}});
        return Arrays.asList(new Object[][]{{true}, {(byte)1}, {(short)1}, {1}, {1L}, {1F}, {1.111D}, {"haloThere"},
                {LocalDate.now()}, {LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS)}, {LocalTime.now().withNano(0).truncatedTo(ChronoUnit.MILLIS)}, {ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS)}});
    }

    @Test
    public void testUpdateWithNulls() {
        if (value instanceof Float) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        }
        if (value instanceof Byte) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteValues());
        }
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", this.value);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name1", this.value);
        this.sqlgGraph.addVertex(T.label, "A", "name1", this.value);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        VertexProperty<String> vp1 = a1.property("name1", null);
        Assert.assertTrue(vp1.isPresent());
        Assert.assertNull(vp1.value());
        VertexProperty<String> vp2 = a1.property("notpresent");
        Assert.assertFalse(vp2.isPresent());
        this.sqlgGraph.tx().commit();
        Vertex a1Again = this.sqlgGraph.traversal().V().hasId(a1.id()).next();
        vp1 = a1Again.property("name1");
        Assert.assertTrue(vp1.isPresent());
        Assert.assertNull(vp1.value());
    }

    @Test
    public void testBatchNormalNullUpdate() {
        if (value instanceof Float) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        }
        if (value instanceof Byte) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteValues());
        }
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "test", "test1", "name1", this.value);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "test", "test2", "name2", this.value);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "test", "test3", "name3", this.value);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        a1.property("name1", this.value);
        a2.property("name2", this.value);
        a3.property("name3", this.value);
        this.sqlgGraph.tx().commit();

        List<Vertex> test1Vertices =  this.sqlgGraph.traversal().V().hasLabel("A").has("test", "test1").toList();
        Assert.assertEquals(1, test1Vertices.size());
        Vertex test1 = test1Vertices.get(0);
        Assert.assertEquals(this.value, test1.value("name1"));
        Assert.assertNull(test1.property("name2").value());
        Assert.assertNull(test1.property("name3").value());

        List<Vertex> test2Vertices =  this.sqlgGraph.traversal().V().hasLabel("A").has("test", "test2").toList();
        Assert.assertEquals(1, test2Vertices.size());
        Vertex test2 = test2Vertices.get(0);
        Assert.assertNull(test2.property("name1").value());
        Assert.assertEquals(this.value, test2.value("name2"));
        Assert.assertNull(test2.property("name3").value());

        List<Vertex> test3Vertices =  this.sqlgGraph.traversal().V().hasLabel("A").has("test", "test3").toList();
        Assert.assertEquals(1, test3Vertices.size());
        Vertex test3 = test3Vertices.get(0);
        Assert.assertNull(test3.property("name1").value());
        Assert.assertNull(test3.property("name2").value());
        Assert.assertEquals(this.value, test3.value("name3"));
    }

}
