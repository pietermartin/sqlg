package org.umlg.sqlg.test;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.AndOrHasContainer;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.SqlgComparatorHolder;
import org.umlg.sqlg.strategy.SqlgRangeHolder;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class TestSchemaTableTreeCache extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSchemaTableTreeCache.class);

//    @Test
//    public void testLRUCAche() {
//        LRUMap<MutableInt, String> lruMap = new LRUMap<>(2);
//        MutableInt mutableInt1 = new MutableInt(1);
//        MutableInt mutableInt2 = new MutableInt(2);
//        MutableInt mutableInt3 = new MutableInt(3);
//        lruMap.put(mutableInt1, "1");
//        lruMap.put(mutableInt2, "2");
//        lruMap.get(mutableInt1);
//        mutableInt1.setValue(33);
//        lruMap.put(mutableInt3, "3");
//        System.out.println("");
//    }

    @Test
    public void testCacheResetOnSchemaChange() {
        Assume.assumeTrue(isPostgres());
        this.sqlgGraph.addVertex(T.label, "A", "prop1", "1");
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> graphTraversal = this.sqlgGraph.traversal().V().hasLabel("A");
        String sql = getSQL(graphTraversal);
        Assert.assertEquals("SELECT\n" +
                "\t\"public\".\"V_A\".\"ID\" AS \"alias1\",\n" +
                "\t\"public\".\"V_A\".\"prop1\" AS \"alias2\"\n" +
                "FROM\n" +
                "\t\"public\".\"V_A\"", sql);

        this.sqlgGraph.addVertex(T.label, "A", "prop2", "2");
        this.sqlgGraph.tx().commit();
        graphTraversal = this.sqlgGraph.traversal().V().hasLabel("A");
        sql = getSQL(graphTraversal);
        Assert.assertEquals("SELECT\n" +
                "\t\"public\".\"V_A\".\"ID\" AS \"alias1\",\n" +
                "\t\"public\".\"V_A\".\"prop2\" AS \"alias2\",\n" +
                "\t\"public\".\"V_A\".\"prop1\" AS \"alias3\"\n" +
                "FROM\n" +
                "\t\"public\".\"V_A\"", sql);

    }

//    @Test
    public void testSchemaTableTreeHashCode() {

        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
            put("a", PropertyDefinition.of(PropertyType.STRING));
        }});
        this.sqlgGraph.tx().commit();

//        SqlgGraph sqlgGraph
//        SchemaTable schemaTable,
//        int stepDepth,
//        List<HasContainer> hasContainers,
//        List<AndOrHasContainer> andOrHasContainers,
//        SqlgComparatorHolder sqlgComparatorHolder,
//        List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> dbComparators,
//        SqlgRangeHolder sqlgRangeHolder,
//        SchemaTableTree.STEP_TYPE stepType,
//        boolean emit,
//        boolean untilFirst,
//        boolean optionalLeftJoin,
//        boolean drop,
//        int replacedStepDepth,
//        Set<String> labels,
//        Pair<String, List<String>> aggregateFunction,
//        List<String> groupBy,
//        boolean idOnly
        SchemaTableTree schemaTableTree1 = new SchemaTableTree(
                this.sqlgGraph,
                null,
                SchemaTable.of("public", "V_A"),
                0,
                List.of(new HasContainer("key1", P.eq("what"))),
                List.of(new AndOrHasContainer(AndOrHasContainer.TYPE.AND)),
                new SqlgComparatorHolder(),
                List.of(),
                SqlgRangeHolder.from(1),
                SchemaTableTree.STEP_TYPE.GRAPH_STEP,
                false,
                false,
                false,
                false,
                1,
                Set.of("label1"),
                Pair.of("String1", List.of("what")),
                List.of("groupBy1"),
                false
        );
        LOGGER.info(Integer.toString(schemaTableTree1.hashCode()));
    }
}
