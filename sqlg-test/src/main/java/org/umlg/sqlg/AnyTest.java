package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithHas;
import org.umlg.sqlg.test.labels.TestHasLabelAndId;
import org.umlg.sqlg.test.schema.TestSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestSchema.class,
        TestHasLabelAndId.class,
        TestGremlinCompileWithHas.class
})
public class AnyTest {
}
