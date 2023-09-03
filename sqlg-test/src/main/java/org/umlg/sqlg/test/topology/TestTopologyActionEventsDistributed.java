package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class TestTopologyActionEventsDistributed extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

//    @Test
//    public void renameVertexLabelBeforeCommit() throws InterruptedException {
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            VertexLabel aVertexLabel = sqlgGraph1.getTopology()
//                    .ensureSchemaExist("A")
//                    .ensureVertexLabelExist("A",
//                            new HashMap<>() {{
//                                put("a", PropertyDefinition.of(PropertyType.STRING));
//                            }}
//                    );
//            VertexLabel bVertexLabel = sqlgGraph1.getTopology().ensureSchemaExist("B")
//                    .ensureVertexLabelExist("B",
//                            new HashMap<>() {{
//                                put("a", PropertyDefinition.of(PropertyType.STRING));
//                            }}
//                    );
//            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
//                    new HashMap<>() {{
//                        put("a", PropertyDefinition.of(PropertyType.STRING));
//                    }}
//            );
//            sqlgGraph1.tx().commit();
//            Thread.sleep(1_000);
//            aVertexLabel.rename("AA");
//            sqlgGraph1.tx().commit();
//            Thread.sleep(1_000_000);
//        }
//    }

    @Test
    public void test() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Schema aSchema = sqlgGraph1.getTopology().ensureSchemaExist("A");
            Schema bSchema = sqlgGraph1.getTopology().ensureSchemaExist("B");
            VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                put(
                        "a1",
                        PropertyDefinition.of(
                                PropertyType.STRING,
                                Multiplicity.of(1, 1, true),
                                "'aa'",
                                "(" + sqlgGraph1.getSqlDialect().maybeWrapInQoutes("a1") + " <> 'a')")
                );
            }});
            PropertyColumn a1PropertyColumn = aVertexLabel.getProperty("a1").orElseThrow();
            aVertexLabel.ensureIndexExists(IndexType.UNIQUE, List.of(a1PropertyColumn));

            VertexLabel aaVertexLabel = aSchema.ensureVertexLabelExist("AA", new LinkedHashMap<>() {{
                put(
                        "a1",
                        PropertyDefinition.of(
                                PropertyType.STRING,
                                Multiplicity.of(1, 1, true),
                                "'aa'",
                                "(" + sqlgGraph1.getSqlDialect().maybeWrapInQoutes("a1") + " <> 'a')")
                );
            }});


            VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
                put(
                        "ab1",
                        PropertyDefinition.of(
                                PropertyType.STRING,
                                Multiplicity.of(1, 1, true),
                                "'ab'",
                                "(" + sqlgGraph1.getSqlDialect().maybeWrapInQoutes("ab1") + " <> 'a')")
                );

            }});
            aaVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
                put(
                        "ab1",
                        PropertyDefinition.of(
                                PropertyType.STRING,
                                Multiplicity.of(1, 1, true),
                                "'ab'",
                                "(" + sqlgGraph1.getSqlDialect().maybeWrapInQoutes("ab1") + " <> 'a')")
                );

            }});
            sqlgGraph1.tx().commit();

            Vertex a = sqlgGraph1.addVertex(T.label, "A.A");
            Vertex b = sqlgGraph1.addVertex(T.label, "B.B");
            a.addEdge("ab", b);
            sqlgGraph1.tx().commit();
            Thread.sleep(2_000);

            aaVertexLabel.remove();
            sqlgGraph1.tx().commit();
            Thread.sleep(1_000_000);

        }

    }

//    @Test
    public void renameEdgeLabelBeforeCommit() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = sqlgGraph1.getTopology()
                    .ensureSchemaExist("A")
                    .ensureVertexLabelExist("A",
                            new HashMap<>() {{
                                put("a", PropertyDefinition.of(PropertyType.STRING));
                            }}
                    );
            VertexLabel aaVertexLabel = sqlgGraph1.getTopology()
                    .ensureSchemaExist("AA")
                    .ensureVertexLabelExist("A",
                            new HashMap<>() {{
                                put("a", PropertyDefinition.of(PropertyType.STRING));
                            }}
                    );
            VertexLabel bVertexLabel = sqlgGraph1.getTopology().ensureSchemaExist("B")
                    .ensureVertexLabelExist("B",
                            new HashMap<>() {{
                                put("a", PropertyDefinition.of(PropertyType.STRING));
                            }}
                    );
            EdgeLabel abEdgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                    new HashMap<>() {{
                        put("a", PropertyDefinition.of(PropertyType.STRING));
                    }}
            );
            EdgeLabel aabEdgeLabel = aaVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                    new HashMap<>() {{
                        put("a", PropertyDefinition.of(PropertyType.STRING));
                    }}
            );
            sqlgGraph1.tx().commit();
            Thread.sleep(1_000);
            aaVertexLabel.remove();
            sqlgGraph1.tx().commit();
            Thread.sleep(1_000_000);
        }
    }
}
