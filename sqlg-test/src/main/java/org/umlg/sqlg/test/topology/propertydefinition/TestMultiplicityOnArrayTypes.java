package org.umlg.sqlg.test.topology.propertydefinition;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;

public class TestMultiplicityOnArrayTypes extends BaseTest {

    @Test
    public void testMultiplicityOnArrayOnVertexLabel() {
        Assume.assumeTrue(sqlgGraph.getSqlDialect().supportsStringArrayValues());
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("a", PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.from(2, 3)));
                }}
        );
        this.sqlgGraph.tx().commit();
        boolean failure = false;
        try {
            this.sqlgGraph.addVertex(T.label, "A", "a", new String[]{"1"});
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);
        failure = false;
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "a", new String[]{"1", "2", "3", "4"});
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);

        failure = false;
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "a", new String[]{"1", "2"});
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertFalse(failure);
    }

    @Test
    public void testMultiplicityOnArrayAndCheckConstraintOnVertexLabel() {
        Assume.assumeTrue(isPostgres());
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("a", PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.from(2, 3), null, "" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("a") + " @> ARRAY['1', '2']"));
                }}
        );
        this.sqlgGraph.tx().commit();
        boolean failure = false;
        try {
            this.sqlgGraph.addVertex(T.label, "A", "a", new String[]{"1"});
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);
        failure = false;
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "a", new String[]{"1", "2", "3", "4"});
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);

        failure = false;
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "a", new String[]{"1", "3"});
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);

        failure = false;
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "a", new String[]{"1", "2", "3"});
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertFalse(failure);
    }
}
