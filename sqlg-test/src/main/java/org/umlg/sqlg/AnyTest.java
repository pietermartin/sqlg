package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.json.JsonTest;
import org.umlg.sqlg.test.localdate.LocalDateTest;
import org.umlg.sqlg.test.schema.TestLazyLoadSchema;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.schema.TestLockedByCurrentThreadPerformance;
import org.umlg.sqlg.test.schema.TestSchemaManagerGetTablesFor;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestSchemaManagerGetTablesFor.class
})
public class AnyTest {
}
