package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
import org.junit.Test;

/**
 * Date: 2014/08/13
 * Time: 10:49 AM
 */
public class TestSchema extends  BaseTest {

    @Test
    public void testSchema() {
        this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA1.Person", "name", "John");
        this.sqlG.tx().commit();
    }

}
