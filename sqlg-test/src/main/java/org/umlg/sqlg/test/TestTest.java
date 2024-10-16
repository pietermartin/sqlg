package org.umlg.sqlg.test;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;

public class TestTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTest.class);

    @BeforeClass
    public static void beforeClass() {
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.addProperty("jdbc.url", "jdbc:postgresql://localhost:5432/cm-dist");
//        configuration.addProperty("jdbc.url", "jdbc:postgresql://10.70.41.151:5432/cm");
        configuration.addProperty("jdbc.username", "cm");
        configuration.addProperty("jdbc.password", "cm");
        SqlgGraph sqlgGraph = SqlgGraph.open(configuration);
        sqlgGraph.close();
    }

    @Before
    public void before() throws Exception {

    }

    @Test
    public void test() {


    }

}
