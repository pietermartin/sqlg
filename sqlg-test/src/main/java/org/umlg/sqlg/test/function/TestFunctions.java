package org.umlg.sqlg.test.function;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.services.SqlgFunctionFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TestFunctions extends BaseTest {

    @Test
    public void testFunctions() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel personVertexLabel = publicSchema.ensureVertexLabelExist("Person",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("surname", PropertyDefinition.of(PropertyType.STRING));
                }});
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "name_" + i, "surname", "surname_" + i);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices =  this.sqlgGraph.traversal().V().hasLabel("Person")
                .<Vertex>call(
                        SqlgFunctionFactory.NAME,
                        Map.of(
                                SqlgFunctionFactory.Params.FUNCTION_AS_STRING_PRODUCER, (Function<Object, String>) o -> {
                                    System.out.println(o);
                                    return "test";
                                }
                        )
                )
                .toList();
    }
}
