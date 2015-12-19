package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestVertexNavToEdges;
import org.umlg.sqlg.test.batch.*;
import org.umlg.sqlg.test.docs.DocumentationUsecases;
import org.umlg.sqlg.test.edges.TestLoadEdge;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.mod.TestRemoveProperty;
import org.umlg.sqlg.test.mod.TestUpdateVertex;
import org.umlg.sqlg.test.mod.TestVertexCreation;
import org.umlg.sqlg.test.vertexout.TestVertexOutWithHas;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestBatch.class,
        TestBatchedStreaming.class,
        TestBatchServerSideEdgeCreation.class,
        TestBatchUpdate.class,
        TestMultiThreadedBatch.class,
        TestStreamingEdge.class,
        TestStreamVertex.class
        })
public class AnyTest {
}
