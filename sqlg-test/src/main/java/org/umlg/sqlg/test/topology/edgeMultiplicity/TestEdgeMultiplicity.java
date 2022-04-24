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

//    @Test
//    public void testMultiplicity() {
//        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
//        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
//        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
//        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
//                new EdgeDefinition(
//                        Multiplicity.from(1, 1),
//                        Multiplicity.from(5, 5)
//                )
//        );
//        VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
//        aVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel,
//                new EdgeDefinition(
//                        Multiplicity.from(0, -1),
//                        Multiplicity.from(0, -1)
//                )
//        );
//
//        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        VertexLabel aaVertexLabel = aSchema.ensureVertexLabelExist("A");
//        VertexLabel bbVertexLabel = aSchema.ensureVertexLabelExist("B");
//        aaVertexLabel.ensureEdgeLabelExist("ab", bbVertexLabel, new EdgeDefinition(Multiplicity.from(1, 1), Multiplicity.from(1, 1)));
//        this.sqlgGraph.tx().commit();
//
//        Optional<EdgeLabel> abEdgeLabelOptional = aVertexLabel.getOutEdgeLabel("ab");
//        Assert.assertTrue(abEdgeLabelOptional.isPresent());
//        EdgeLabel abEdgeLabel = abEdgeLabelOptional.get();
//        Set<EdgeRole> inEdgeRoles = abEdgeLabel.getInEdgeRoles();
//        Assert.assertEquals(2, inEdgeRoles.size());
//        Set<EdgeRole> outEdgeRoles = abEdgeLabel.getOutEdgeRoles();
//        Assert.assertEquals(1, outEdgeRoles.size());
//        Assert.assertEquals(0, abEdgeLabel.getProperties().size());
//        Set<EdgeRole> outEdgeRolesForVertexLabel = abEdgeLabel.getOutEdgeRoles(aVertexLabel);
//        Assert.assertEquals(1, outEdgeRolesForVertexLabel.size());
//        Assert.assertEquals(Multiplicity.from(1, 1), outEdgeRolesForVertexLabel.iterator().next().getMultiplicity());
//        Set<EdgeRole> inEdgeRolesForVertexLabel = abEdgeLabel.getInEdgeRoles(bVertexLabel);
//        Assert.assertEquals(1, inEdgeRolesForVertexLabel.size());
//        Assert.assertEquals(Multiplicity.from(5, 5), inEdgeRolesForVertexLabel.iterator().next().getMultiplicity());
//    }

    @Test
    public void testLoadMultiplicity() {
        try (SqlgGraph sqlgGraph2 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = sqlgGraph2.getTopology().getPublicSchema().ensureVertexLabelExist("A");
            VertexLabel bVertexLabel = sqlgGraph2.getTopology().getPublicSchema().ensureVertexLabelExist("B");
            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new EdgeDefinition(Multiplicity.from(5, 5), Multiplicity.from(5, 5)));
            sqlgGraph2.tx().commit();
        }
        try (SqlgGraph sqlgGraph2 = SqlgGraph.open(configuration)) {
            Optional<VertexLabel> optionalAVertexLabel = sqlgGraph2.getTopology().getPublicSchema().getVertexLabel("A");
            Assert.assertTrue(optionalAVertexLabel.isPresent());
            Optional<VertexLabel> optionalBVertexLabel = sqlgGraph2.getTopology().getPublicSchema().getVertexLabel("B");
            Assert.assertTrue(optionalBVertexLabel.isPresent());
            Optional<EdgeLabel> optionalEdgeLabel = sqlgGraph2.getTopology().getPublicSchema().getEdgeLabel("ab");
            Assert.assertTrue(optionalEdgeLabel.isPresent());
            Set<EdgeRole> edgeRoles = optionalEdgeLabel.get().getOutEdgeRoles(optionalAVertexLabel.get());
            Assert.assertEquals(1, edgeRoles.size());
            Multiplicity multiplicity = edgeRoles.iterator().next().getMultiplicity();
            Assert.assertEquals(Multiplicity.from(5,5), multiplicity);
            edgeRoles = optionalEdgeLabel.get().getInEdgeRoles(optionalBVertexLabel.get());
            Assert.assertEquals(1, edgeRoles.size());
            Assert.assertEquals(Multiplicity.from(5,5), multiplicity);
        }
    }
}
