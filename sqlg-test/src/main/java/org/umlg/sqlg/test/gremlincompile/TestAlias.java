package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Date: 2015/12/14
 * Time: 10:15 PM
 */
public class TestAlias extends BaseTest {

    @Test
    public void testFieldWithDots() {
        this.sqlgGraph.addVertex(T.label, "Person", "test.1", "a");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(1, vertices.size());
        assertEquals("a", vertices.get(0).value("test.1"));
        assertNotNull(vertices.get(0).property("test.1").value());
    }

    @Test
    public void testAlias() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> result = this.sqlgGraph.traversal()
                .V()
                .hasLabel("A").as("a", "aa", "aaa")
                .out("ab").as("b", "bb", "bbb")
                .<Vertex>select("a", "aa", "aaa", "b", "bb", "bbb")
                .toList();

        for (Map<String, Vertex> stringVertexMap : result) {
            for (Map.Entry<String, Vertex> stringVertexEntry : stringVertexMap.entrySet()) {
                String label = stringVertexEntry.getKey();
                Vertex vertex = stringVertexEntry.getValue();
                System.out.println("label = " + label + ", vertex name =  " + vertex.value("name"));
            }
        }

//        alias1
//        2P~~~b~&~2P~~~bb~&~2P~~~bbb~&~public~&~V_B~&~ID

//        alias2
//        2P~~~b~&~2P~~~bb~&~2P~~~bbb~&~public~&~V_B~&~name

//        alias3
//        1P~~~aa~&~1P~~~aaa~&~1P~~~a~&~public~&~V_A~&~ID

//        alias4
//        1P~~~aa~&~1P~~~aaa~&~1P~~~a~&~public~&~V_A~&~name
    }
}
