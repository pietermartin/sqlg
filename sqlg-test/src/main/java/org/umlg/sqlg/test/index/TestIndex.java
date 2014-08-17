package org.umlg.sqlg.test.index;

import com.tinkerpop.gremlin.structure.Element;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/08/17
 * Time: 2:43 PM
 */
public class TestIndex extends BaseTest {

    @Test
    public void testIndex() {
        this.sqlG.createLabeledIndex("Person", "name1", "dummy", "name2", "dummy", "name3", "dummy");
        this.sqlG.tx().commit();
        for (int i = 0; i < 100; i++) {
            this.sqlG.addVertex(Element.LABEL, "Person", "name1", "john" + i, "name2", "tom" + i, "name3", "piet" + i);
            if (i % 10000 == 0) {
//                System.out.println("commit " + i);
                this.sqlG.tx().commit();
            }
        }
        this.sqlG.tx().commit();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Person").has("name1", "john50").count().next(), 0);
        stopWatch.stop();
//        System.out.println(stopWatch.toString());
    }
}
