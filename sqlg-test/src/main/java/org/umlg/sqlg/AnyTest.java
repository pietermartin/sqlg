package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.TestBatchJson;
import org.umlg.sqlg.test.json.TestJson;
import org.umlg.sqlg.test.json.TestJsonUpdate;
import org.umlg.sqlg.test.mod.TestRemoveProperty;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.vertex.TestAddTemporaryVertex;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestLoadSchema.class,
        TestJson.class,
        TestRemoveProperty.class,
        TestBatchJson.class,
        TestJsonUpdate.class,
        TestAddTemporaryVertex.class
})
public class AnyTest {
}
