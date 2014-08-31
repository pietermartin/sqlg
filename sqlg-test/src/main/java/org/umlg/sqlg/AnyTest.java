package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.*;
import org.umlg.sqlg.test.index.TestIndex;
import org.umlg.sqlg.test.mod.TestUpdateVertex;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.schema.TestSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestSchema.class
})
public class AnyTest {
}
