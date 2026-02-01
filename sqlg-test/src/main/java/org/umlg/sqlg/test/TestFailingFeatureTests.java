package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class TestFailingFeatureTests extends BaseTest {

    @Test
    public void g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_maxXlocalX() {
        loadModern();
//          # It verifies if empty lists are filtered out as expected
//        Scenario: g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_maxXlocalX
//        Given the modern graph
//        And the traversal of
//        """
//        """
//        When iterated to list
//        Then the result should be unordered
//      | result |
//                | d[29].d |
//                | d[27].i |
//                | d[32].d |
//                | d[35].d |

        for (int i = 0; i < 100; i++) {
            Set<Comparable> comparables = this.sqlgGraph.traversal().V().local(__.union(__.values("age"), __.outE().values("weight")).fold()).max(Scope.local).toSet();
            Assert.assertEquals(4, comparables.size());
            Assert.assertTrue(comparables.contains(29D));
            Assert.assertTrue(comparables.contains(27));
            Assert.assertTrue(comparables.contains(32D));
            Assert.assertTrue(comparables.contains(35D));
        }

    }

    @Test
    public void g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_meanXlocalX() {
        loadModern();
//  # It verifies if empty lists are filtered out as expected
//        Scenario: g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_meanXlocalX
//        Given the modern graph
//        And the traversal of
//        """
//        g.V().local(union(values("age"), outE().values("weight")).fold()).mean(local)
//        """
//        When iterated to list
//        Then the result should be unordered
//      | result |
//                | d[7.725].d |
//                | d[27.0].d |
//                | d[11.133333333333333].d |
//                | d[17.6].d |

        Set<Number> numbers = this.sqlgGraph.traversal().V().local(__.union(__.values("age"), __.outE().values("weight")).fold()).mean(Scope.local).toSet();
        Assert.assertEquals(4, numbers.size());
        Assert.assertTrue(numbers.contains(7.725D));
        Assert.assertTrue(numbers.contains(27D));
        Assert.assertTrue(numbers.contains(11.133333333333333D));
        Assert.assertTrue(numbers.contains(17.6D));
    }


    @Test
    public void g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_minXlocalX() {
        loadModern();
//  # It verifies if empty lists are filtered out as expected
//        Scenario: g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_minXlocalX
//        Given the modern graph
//        And the traversal of
//        """
//        g.V().local(union(values("age"), outE().values("weight")).fold()).min(local)
//        """
//        When iterated to list
//        Then the result should be unordered
//      | result |
//                | d[0.4].d |
//                | d[27].i |
//                | d[0.4].d |
//                | d[0.2].d |

        List<Comparable> comparables = this.sqlgGraph.traversal().V().local(__.union(__.values("age"), __.outE().values("weight")).fold()).min(Scope.local).toList();
        Assert.assertEquals(4, comparables.size());
        Assert.assertTrue(comparables.contains(0.4D));
        Assert.assertTrue(comparables.contains(27));
        Assert.assertTrue(comparables.contains(0.4D));
        Assert.assertTrue(comparables.contains(0.2D));
    }

    @Test
    public void g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_sumXlocalX() {
        loadModern();
//            # It verifies if empty lists are filtered out as expected
//        Scenario: g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_sumXlocalX
//        Given the modern graph
//        And the traversal of
//        """
//        g.V().local(union(values("age"), outE().values("weight")).fold()).sum(local)
//        """
//        When iterated to list
//        Then the result should be unordered
//      | result |
//                | d[30.9].d |
//                | d[27].i |
//                | d[33.4].d |
//                | d[35.2].d |

        Set<Number> numbers = this.sqlgGraph.traversal().V().local(__.union(__.values("age"), __.outE().values("weight")).fold()).sum(Scope.local).toSet();
        Assert.assertEquals(4, numbers.size());
        Assert.assertTrue(numbers.contains(30.9D));
        Assert.assertTrue(numbers.contains(27));
        Assert.assertTrue(numbers.contains(33.4D));
        Assert.assertTrue(numbers.contains(35.2D));

    }





}

