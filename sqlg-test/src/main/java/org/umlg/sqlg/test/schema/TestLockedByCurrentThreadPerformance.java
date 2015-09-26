package org.umlg.sqlg.test.schema;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * Created by pieter on 2015/09/25.
 */
public class TestLockedByCurrentThreadPerformance extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    //TODO sqlg needs proper benchmark support, this test should somehow make a performance penalty visible
    @Test
    public void testGetAllTables() throws Exception {
        //Create a new sqlgGraph
        SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration);
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode()) {
            this.sqlgGraph.tx().batchModeOn();
        }
        for (int i = 0; i < 100000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name1", i, "name2", i);
        }
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(100000, persons.size());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        sqlgGraph1.close();
    }
}
