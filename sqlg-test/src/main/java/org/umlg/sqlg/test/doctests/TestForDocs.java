package org.umlg.sqlg.test.doctests;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;

/**
 * Date: 2014/12/14
 * Time: 3:29 PM
 */
@SuppressWarnings("Duplicates")
public class TestForDocs extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestForDocs.class);

//    @Test
//    public void testNameIsRequired() {
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
//                .ensureVertexLabelExist("Person",
//                        new HashMap<>() {{
//                            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1)));
//                        }}
//                );
//        boolean failure = false;
//        try {
//            this.sqlgGraph.addVertex(T.label, "Person");
//            this.sqlgGraph.tx().commit();
//        } catch (Exception e) {
//            LOGGER.error(e.getMessage(), e);
//            failure = true;
//        }
//        assertTrue(failure);
//    }

@Test
public void testCheckConstraints() {
    this.sqlgGraph.getTopology().getPublicSchema()
            .ensureVertexLabelExist("Person",
                    new HashMap<>() {{
                        put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(), "'Peter'", "name <> 'John'"));
                    }}
            );
    this.sqlgGraph.tx().commit();
    this.sqlgGraph.addVertex(T.label, "Person");
    this.sqlgGraph.tx().commit();
    boolean failure = false;
    try {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
        this.sqlgGraph.tx().commit();
    } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        failure = true;
    }
    assertTrue(failure);
}

}
