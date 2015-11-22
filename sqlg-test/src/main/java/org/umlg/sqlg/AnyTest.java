package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestAddVertexViaMap;
import org.umlg.sqlg.test.TestAllEdges;
import org.umlg.sqlg.test.TestVertexNavToEdges;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestStreamVertex;
import org.umlg.sqlg.test.graph.TestGraphStepWithIds;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.mod.TestDeletedVertex;
import org.umlg.sqlg.test.vertex.TestNewVertex;
import org.umlg.sqlg.test.vertex.TestOtherVertex;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestRepeatStepGraphOut.class
        })
public class AnyTest {
}
