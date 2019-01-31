package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.schema.TestMultiThread;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestReadOnlyRole.class,
//        TestMultipleThreadMultipleJvm.class,
//        TestDataSource.class,
//        TestDeadLock.class,
//        TestDataSource.class,
//        TestMultipleThreadMultipleJvm.class,
        TestMultiThread.class,
//        TestNotifyJson.class,
//        TestLoadSchemaViaNotify.class,
//        TestMultiThreadedBatch.class,
//        TestBatchStreamTemporaryVertex.class
})
public class AnyTest {
}
