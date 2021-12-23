package org.umlg.sqlg.ui.test;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;
import org.umlg.sqlg.ui.SqlgUI;

import java.time.LocalDateTime;
import java.util.*;

public class SqlgUITest extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
//        SqlgUI.initialize(8181);
        SqlgUI.initialize();
        SqlgUI.set(this.sqlgGraph);
    }

    @Test
    public void test() throws InterruptedException {
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        Vertex nowhere = this.sqlgGraph.addVertex(T.label, "Home", "address", "Nowhere");
        Vertex aVertex = this.sqlgGraph.addVertex(T.label, "A.A", "field1", "1");
        john.addEdge("livesAt", nowhere, "createdOn", LocalDateTime.now());
        aVertex.addEdge("diesAt", nowhere, "createdOn", LocalDateTime.now());

        VertexLabel test1VertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Test1", new LinkedHashMap<>() {{
                    put("name1", PropertyType.STRING);
                    put("name2", PropertyType.STRING);
                    put("name3", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("age", PropertyType.LONG);
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2", "name3"))
        );
        Optional<PropertyColumn> name1PropertyColumnOpt = test1VertexLabel.getProperty("name1");
        test1VertexLabel.ensureIndexExists(IndexType.UNIQUE, List.of(name1PropertyColumnOpt.get()));

        VertexLabel test2VertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Test2", new LinkedHashMap<>() {{
                    put("name1", PropertyType.STRING);
                    put("name2", PropertyType.STRING);
                    put("name3", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("age", PropertyType.LONG);
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2", "name3"))
        );
        name1PropertyColumnOpt = test2VertexLabel.getProperty("name1");
        Optional<PropertyColumn> name2PropertyColumnOpt = test2VertexLabel.getProperty("name2");
        test1VertexLabel.ensureIndexExists(IndexType.UNIQUE, List.of(name1PropertyColumnOpt.get(), name2PropertyColumnOpt.get()));

        EdgeLabel loveEdgeLabel = test1VertexLabel.ensureEdgeLabelExist("loves", test2VertexLabel, new HashMap<>() {{
            put("p1", PropertyType.STRING);
            put("p2", PropertyType.STRING);
        }});
        Optional<PropertyColumn> p1PropertyColumn = loveEdgeLabel.getProperty("p1");
        Optional<PropertyColumn> p2PropertyColumn = loveEdgeLabel.getProperty("p2");
        loveEdgeLabel.ensureIndexExists(IndexType.UNIQUE, List.of(p1PropertyColumn.get(), p2PropertyColumn.get()));

        this.sqlgGraph.tx().commit();

        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "int1", "int2", "int3")),
                PartitionType.LIST,
                "int1");
        Partition p1 = a.ensureListPartitionWithSubPartitionExists("int1", "1,2,3,4,5", PartitionType.LIST, "int2");
        Partition p2 = a.ensureListPartitionWithSubPartitionExists("int2", "6,7,8,9,10", PartitionType.LIST, "int2");

        Partition p1_1 = p1.ensureListPartitionWithSubPartitionExists("int11", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p1_2 = p1.ensureListPartitionWithSubPartitionExists("int12", "6,7,8,9,10", PartitionType.LIST, "int3");
        Partition p2_1 = p2.ensureListPartitionWithSubPartitionExists("int21", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p2_2 = p2.ensureListPartitionWithSubPartitionExists("int22", "6,7,8,9,10", PartitionType.LIST, "int3");

        p1_1.ensureListPartitionExists("int111", "1,2,3,4,5");
        p1_1.ensureListPartitionExists("int112", "6,7,8,9,10");
        p1_2.ensureListPartitionExists("int121", "1,2,3,4,5");
        p1_2.ensureListPartitionExists("int122", "6,7,8,9,10");
        p2_1.ensureListPartitionExists("int211", "1,2,3,4,5");
        p2_1.ensureListPartitionExists("int212", "6,7,8,9,10");
        p2_2.ensureListPartitionExists("int221", "1,2,3,4,5");
        p2_2.ensureListPartitionExists("int222", "6,7,8,9,10");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "int1", 5, "int2", 5, "int3", 5);

        this.sqlgGraph.tx().commit();
        System.out.println("running...");
        long l = 1000 * 60 * 60 * 5;
        Thread.sleep(l);
    }
}
