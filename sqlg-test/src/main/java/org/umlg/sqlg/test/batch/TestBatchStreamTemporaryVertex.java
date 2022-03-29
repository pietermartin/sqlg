package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2015/12/31
 * Time: 9:14 AM
 */
public class TestBatchStreamTemporaryVertex extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
    }

    @Test
    public void testTempBatch() throws SQLException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 1000; i++) {
            this.sqlgGraph.streamTemporaryVertex("halo", new LinkedHashMap<String, Object>() {{
                put("this", "that");
            }});
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement s = conn.prepareStatement("select * from \"V_halo\"")) {
            Assert.assertEquals("", s.getMetaData().getSchemaName(1));
            ResultSet resultSet = s.executeQuery();
            while (resultSet.next()) {
                count++;
                Assert.assertEquals("that", resultSet.getString(2));
            }
        }
        Assert.assertEquals(1000, count);
        this.sqlgGraph.tx().commit();

    }

    //Testing issue #226
    @Test
    public void testStreamTemporaryVertexMultipleThreads() throws InterruptedException {

        VertexLabel haloVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("halo");
        haloVertexLabel.ensurePropertiesExist(new HashMap<>() {{
            put("this", PropertyDefinition.of(PropertyType.STRING));
        }});
        this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
        this.sqlgGraph.tx().commit();

        final CountDownLatch countDownLatch1 = new CountDownLatch(1);
        final CountDownLatch countDownLatch2 = new CountDownLatch(1);

        final Thread thread1 = new Thread("thread1") {
            @Override
            public void run() {
                TestBatchStreamTemporaryVertex.this.sqlgGraph.tx().streamingBatchModeOn();
                TestBatchStreamTemporaryVertex.this.sqlgGraph.streamTemporaryVertex("halo", new LinkedHashMap<>() {{
                    put("this", "that");
                }});
                countDownLatch1.countDown();
                System.out.println("countDownLatch1 countDown");
                TestBatchStreamTemporaryVertex.this.sqlgGraph.streamTemporaryVertex("halo", new LinkedHashMap<>() {{
                    put("this", "that");
                }});
                try {
                    countDownLatch2.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    //swallow
                }
                //If Topology.temporaryTable has been cleared then the next line will block.
                //It will block because it will try to create the temp table but the copy command is already in progress.
                //The copy command needs to finish before the driver will allow any other command to execute.
                TestBatchStreamTemporaryVertex.this.sqlgGraph.streamTemporaryVertex("halo", new LinkedHashMap<>() {{
                    put("this", "that");
                }});
                TestBatchStreamTemporaryVertex.this.sqlgGraph.tx().commit();
            }
        };
        thread1.start();
        final Thread thread2 = new Thread("thread2") {
            @Override
            public void run() {
                try {
                    countDownLatch1.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("thread2 countDownLatch ");
                TestBatchStreamTemporaryVertex.this.sqlgGraph.addVertex(T.label, "A");
                TestBatchStreamTemporaryVertex.this.sqlgGraph.tx().commit();
                countDownLatch2.countDown();
            }
        };
        thread2.start();

        thread1.join();
        thread2.join();
    }
}
