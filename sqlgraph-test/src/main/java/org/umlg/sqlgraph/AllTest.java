package org.umlg.sqlgraph;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlgraph.test.*;

/**
 * Date: 2014/07/16
 * Time: 12:08 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestAllEdges.class,
        TestAllVertices.class,
        TestEdgeCreation.class,
        TestGetById.class,
        TestHas.class,
        TestLoadElementProperties.class,
        TestRemoveElement.class,
        TestRemoveProperty.class,
        TestSetProperty.class,
        TestVertexCreation.class,
        TestVertexNavToEdges.class,
        TinkerpopTest.class
})
public class AllTest {
}
