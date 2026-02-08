package org.umlg.sqlg.test.vertex;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;

public class TestValidateProperty extends BaseTest {

    @Test
    public void testThrowExceptionForNullChar() {
        Assume.assumeTrue(isPostgres());
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "\0x00");
            Assert.fail("expected exception");
        } catch (IllegalArgumentException e) {
            //noop
        }
    }

    @Test
    public void testThrowExceptionForNullCharBatchMode() {
        Assume.assumeTrue(isPostgres());
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "\0x00");
            Assert.fail("expected exception");
        } catch (IllegalArgumentException e) {
            //noop
        }
    }

    @Test
    public void testThrowExceptionForNullCharStreamingBatchMode() {
        Assume.assumeTrue(isPostgres());
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        try {
            this.sqlgGraph.streamVertex(T.label, "A", "name", "\0x00");
            Assert.fail("expected exception");
        } catch (IllegalArgumentException e) {
            //noop
        }
    }
}
