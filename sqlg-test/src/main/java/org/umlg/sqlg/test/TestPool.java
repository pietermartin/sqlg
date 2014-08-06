package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
import org.junit.Test;

/**
 * Date: 2014/07/16
 * Time: 7:43 PM
 */
public class TestPool extends BaseTest {

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
