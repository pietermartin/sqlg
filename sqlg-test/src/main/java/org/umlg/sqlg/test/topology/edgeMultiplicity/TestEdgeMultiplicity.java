package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.Optional;
import java.util.Set;

public class TestEdgeMultiplicity extends BaseTest {

    @Test
    public void testX() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel ab1 = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        EdgeLabel ab2 = bVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel);
        this.sqlgGraph.tx().commit();
        System.out.println("");

    }

//    @Test
    public void test() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel abEdgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.from(0, -1),
                        Multiplicity.from(1, 1)
                )
        );
        VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
        aVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel,
                new EdgeDefinition(
                        Multiplicity.from(0, -1),
                        Multiplicity.from(0, -1)
                )
        );

        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aaVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bbVertexLabel = aSchema.ensureVertexLabelExist("B");
        aaVertexLabel.ensureEdgeLabelExist("ab", bbVertexLabel);
        this.sqlgGraph.tx().commit();

        Optional<EdgeLabel> abEdgeLabelOptional = aVertexLabel.getOutEdgeLabel("ab");
        Assert.assertTrue(abEdgeLabelOptional.isPresent());
        abEdgeLabel = abEdgeLabelOptional.get();
        Set<EdgeRole> inEdgeRoles = abEdgeLabel.getInEdgeRoles();
        Assert.assertEquals(2, inEdgeRoles.size());
        Set<EdgeRole> outEdgeRoles = abEdgeLabel.getOutEdgeRoles();
        Assert.assertEquals(1, outEdgeRoles.size());
        Assert.assertEquals(0, abEdgeLabel.getProperties().size());

        Optional<EdgeLabel> aabEdgeLabelOptional = aaVertexLabel.getOutEdgeLabel("ab");
        Assert.assertTrue(aabEdgeLabelOptional.isPresent());
        EdgeLabel aabEdgeLabel = aabEdgeLabelOptional.get();
        System.out.println("");
    }
}
