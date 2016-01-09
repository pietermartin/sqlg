package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestCountVerticesAndEdges;
import org.umlg.sqlg.test.batch.*;
import org.umlg.sqlg.test.docs.DocumentationUsecases;
import org.umlg.sqlg.test.edges.TestEdgeCache;
import org.umlg.sqlg.test.edges.TestForeignKeysAreOptional;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWhere;
import org.umlg.sqlg.test.gremlincompile.TestRepeatStepGraphOut;
import org.umlg.sqlg.test.index.TestIndex;
import org.umlg.sqlg.test.localdate.LocalDateTest;
import org.umlg.sqlg.test.mod.TestVertexCreation;
import org.umlg.sqlg.test.rollback.TestRollback;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.schema.TestSchema;
import org.umlg.sqlg.test.schema.TestSqlgSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestLoadSchema.class
        })
public class AnyTest {
}
