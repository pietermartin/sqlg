package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

/**
 * Created by pieter on 2015/12/09.
 */
public class TestSqlgSchema  extends BaseTest {

    @Test
    public void testSqlgSchemaExist() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().count().next(), 0);

        SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration);

        GraphTraversalSource traversalSource = GraphTraversalSource.build().with(
                TopologyStrategy.build().selectFrom(
                        SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES
                ).create()
        ).create(sqlgGraph1);
        GraphTraversal<Vertex, Vertex> gt = traversalSource.V().hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA).has("name", "public");
        Assert.assertTrue(gt.hasNext());
    }

}
