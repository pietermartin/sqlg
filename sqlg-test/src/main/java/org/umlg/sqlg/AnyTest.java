package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestStreamVertex;
import org.umlg.sqlg.test.gremlincompile.TestBulkWithin;
import org.umlg.sqlg.test.gremlincompile.TestBulkWithout;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWhere;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithHas;
import org.umlg.sqlg.test.json.JsonTest;
import org.umlg.sqlg.test.mod.TestRemoveProperty;
import org.umlg.sqlg.test.schema.TestLoadSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestBulkWithin.class
        })
public class AnyTest {
}
