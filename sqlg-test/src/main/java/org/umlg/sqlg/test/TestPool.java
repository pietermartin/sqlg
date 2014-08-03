package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlGDataSource;

import java.beans.PropertyVetoException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Date: 2014/07/16
 * Time: 7:43 PM
 */
public class TestPool extends BaseTest {

    @Test
    public void testPool() throws ConfigurationException, SQLException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlgraph.properties");
        Configuration config = new PropertiesConfiguration(sqlProperties);
        SqlGDataSource.INSTANCE.setupDataSource(
                config.getString("jdbc.driver"),
                config.getString("jdbc.url"),
                config.getString("jdbc.username"),
                config.getString("jdbc.password"));
        for (int i = 0; i < 100; i++) {
            Connection conn = SqlGDataSource.INSTANCE.get(config.getString("jdbc.url")).getConnection();
            Statement st = conn.createStatement();
            st.execute("select * from pg_catalog.pg_aggregate");
            st.close();
            conn.close();
        }
    }

    @Test
    public void testSqlGraphConnectionsDoesNotExhaustPool() {
        for (int i = 0; i < 1000; i++) {
            this.sqlG.addVertex(Element.LABEL, "Person");
        }
        this.sqlG.tx().commit();
        for (int i = 0; i < 1000; i++) {
            this.sqlG.V().has(Element.LABEL, "Person").hasNext();
        }
    }
}
