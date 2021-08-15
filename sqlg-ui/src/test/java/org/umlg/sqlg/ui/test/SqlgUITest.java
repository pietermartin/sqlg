package org.umlg.sqlg.ui.test;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.IndexType;
import org.umlg.sqlg.structure.topology.PartitionType;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDateTime;
import java.util.*;

public class SqlgUITest extends BaseTest {

    @Test
    public void test() throws InterruptedException {
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        Vertex nowhere = this.sqlgGraph.addVertex(T.label, "Home", "address", "Nowhere");
        Vertex aVertex = this.sqlgGraph.addVertex(T.label, "A.A", "field1", "1");
        john.addEdge("livesAt", nowhere, "createdOn", LocalDateTime.now());
        aVertex.addEdge("diesAt", nowhere, "createdOn", LocalDateTime.now());

        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Test", new LinkedHashMap<>() {{
                    put("name1", PropertyType.STRING);
                    put("name2", PropertyType.STRING);
                    put("name3", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("age", PropertyType.LONG);
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2", "name3"))
        );
        Optional<PropertyColumn> name1PropertyColumnOpt = vertexLabel.getProperty("name1");
        vertexLabel.ensureIndexExists(IndexType.UNIQUE, List.of(name1PropertyColumnOpt.get()));
        this.sqlgGraph.tx().commit();


        this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "P1",
                new HashMap<>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Set.of("name")),
                PartitionType.LIST,
                "name");

        System.out.println("asd");
        long l = 1000 * 60 * 60 * 5;
        Thread.sleep(l);
    }
}
