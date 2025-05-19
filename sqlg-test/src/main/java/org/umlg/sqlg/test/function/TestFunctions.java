package org.umlg.sqlg.test.function;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.services.SqlgFunctionFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
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
                    put("age", PropertyDefinition.of(PropertyType.INTEGER));
                }});
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "name_" + i, "surname", "surname_" + i, "age", i);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person")
                .<Vertex>call(
                        SqlgFunctionFactory.NAME,
                        Map.of(
                                SqlgFunctionFactory.Params.COLUMN_NAME, "addition",
                                SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, PropertyType.LONG,
                                SqlgFunctionFactory.Params.FUNCTION_AS_STRING, (Function<Object, String>) o -> "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("age") + " + 10)"
                        )
                )
                .toList();
        Map<RecordId, Long> recordIdAdditionMap = new HashMap<>();
        Assert.assertEquals(10, vertices.size());
        for (Vertex vertex : vertices) {
            Integer age = vertex.value("age");
            Long addition = vertex.value("addition");
            Assert.assertEquals(addition, age + 10, 0);
            recordIdAdditionMap.put((RecordId) vertex.id(), addition);
        }
        List<Map<Object, Object>> additions = this.sqlgGraph.traversal().V().hasLabel("Person")
                .<Vertex>call(
                        SqlgFunctionFactory.NAME,
                        Map.of(
                                SqlgFunctionFactory.Params.COLUMN_NAME, "addition",
                                SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, PropertyType.LONG,
                                SqlgFunctionFactory.Params.FUNCTION_AS_STRING, (Function<Object, String>) o -> "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("age") + " + 10)"
                        )
                )
                .elementMap("addition")
                .toList();
        Assert.assertEquals(10, additions.size());
        for (Map<Object, Object> additionMap : additions) {
            RecordId recordId = (RecordId) additionMap.get(T.id);
            Long addition = (Long) additionMap.get("addition");
            Assert.assertEquals(addition, recordIdAdditionMap.get(recordId));
        }

        List<Vertex> _additions = this.sqlgGraph.traversal().V().hasLabel("Person")
                .<Vertex>fun(
                        "addition",
                        PropertyType.LONG,
                        o -> "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("age") + " + 10)"
                )
                .toList();
        Assert.assertEquals(vertices, _additions);
    }
}
