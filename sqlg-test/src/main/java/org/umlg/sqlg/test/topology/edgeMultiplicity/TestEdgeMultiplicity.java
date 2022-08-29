package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.Optional;
import java.util.Set;

public class TestEdgeMultiplicity extends BaseTest {

    @Test
    public void testMultiplicity() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(5, 5)
                )
        );
        VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
        aVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(0, -1)
                )
        );

        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aaVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bbVertexLabel = aSchema.ensureVertexLabelExist("B");
        aaVertexLabel.ensureEdgeLabelExist("ab",
                bbVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(1, 1))
        );
        this.sqlgGraph.tx().commit();

        Optional<EdgeLabel> abEdgeLabelOptional = aVertexLabel.getOutEdgeLabel("ab");
        Assert.assertTrue(abEdgeLabelOptional.isPresent());
        EdgeLabel abEdgeLabel = abEdgeLabelOptional.get();
        Set<EdgeRole> inEdgeRoles = abEdgeLabel.getInEdgeRoles();
        Assert.assertEquals(2, inEdgeRoles.size());
        Set<EdgeRole> outEdgeRoles = abEdgeLabel.getOutEdgeRoles();
        Assert.assertEquals(1, outEdgeRoles.size());
        Assert.assertEquals(0, abEdgeLabel.getProperties().size());
        EdgeRole outEdgeRoleForVertexLabel = abEdgeLabel.getOutEdgeRoles(aVertexLabel);
        Assert.assertNotNull(outEdgeRoleForVertexLabel);
        Assert.assertEquals(Multiplicity.of(1, 1), outEdgeRoleForVertexLabel.getMultiplicity());
        EdgeRole inEdgeRoleForVertexLabel = abEdgeLabel.getInEdgeRoles(bVertexLabel);
        Assert.assertNotNull(inEdgeRoleForVertexLabel);
        Assert.assertEquals(Multiplicity.of(5, 5), inEdgeRoleForVertexLabel.getMultiplicity());
    }

    @Test
    public void testLoadMultiplicity() {
        try (SqlgGraph sqlgGraph2 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = sqlgGraph2.getTopology().getPublicSchema().ensureVertexLabelExist("A");
            VertexLabel bVertexLabel = sqlgGraph2.getTopology().getPublicSchema().ensureVertexLabelExist("B");
            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                    new EdgeDefinition(
                            Multiplicity.of(5, 5),
                            Multiplicity.of(5, 5))
            );
            sqlgGraph2.tx().commit();
        }
        try (SqlgGraph sqlgGraph2 = SqlgGraph.open(configuration)) {
            Optional<VertexLabel> optionalAVertexLabel = sqlgGraph2.getTopology().getPublicSchema().getVertexLabel("A");
            Assert.assertTrue(optionalAVertexLabel.isPresent());
            Optional<VertexLabel> optionalBVertexLabel = sqlgGraph2.getTopology().getPublicSchema().getVertexLabel("B");
            Assert.assertTrue(optionalBVertexLabel.isPresent());
            Optional<EdgeLabel> optionalEdgeLabel = sqlgGraph2.getTopology().getPublicSchema().getEdgeLabel("ab");
            Assert.assertTrue(optionalEdgeLabel.isPresent());
            EdgeRole edgeRole = optionalEdgeLabel.get().getOutEdgeRoles(optionalAVertexLabel.get());
            Assert.assertNotNull(edgeRole);
            Multiplicity multiplicity = edgeRole.getMultiplicity();
            Assert.assertEquals(Multiplicity.of(5, 5), multiplicity);
            edgeRole = optionalEdgeLabel.get().getInEdgeRoles(optionalBVertexLabel.get());
            Assert.assertNotNull(edgeRole);
            Assert.assertEquals(Multiplicity.of(5, 5), multiplicity);
        }
    }


}
