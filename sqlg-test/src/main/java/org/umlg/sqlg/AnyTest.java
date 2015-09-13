package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.json.JsonTest;
import org.umlg.sqlg.test.localdate.LocalDateTest;
import org.umlg.sqlg.test.schema.TestLoadSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestTreeStep.class
})
public class AnyTest {
}
