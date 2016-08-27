package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.AnyTest;
import org.umlg.sqlg.test.schema.TestLoadSchema;

/**
 * @author Lukas Krejci
 * @since 1.3.0
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
         TestLoadSchema.class
})
public class H2AnyTest extends AnyTest {
}
