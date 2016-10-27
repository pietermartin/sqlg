package org.umlg.sqlg.test.vertex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.BatchManager.BatchModeType;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/11/19
 * Time: 6:34 PM
 */
public class TestOtherVertex extends BaseTest {

    @Test
    public void testOtherV() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Edge e1 = a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1).outE().otherV().path().toList();
        assertEquals(1, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(e1) && p.get(2).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }
    
    /**
     * test two Java instances of the same vertex are equals and have same hashcode
     */
    @Test
    public void testEqualsHashcode(){
    	Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
    	this.sqlgGraph.tx().commit();
    	assertNotNull(a1.id());
    	Vertex a2=this.sqlgGraph.traversal().V(a1.id()).next();
    	assertNotSame(a1, a2);
    	assertEquals(a1,a2);
    	assertEquals(a1.hashCode(),a2.hashCode());
    }
    
    /**
     * test two Java instances of the same vertex are equals and have same hashcode,in batch mode
     */
    @Test
    public void testEqualsHashcodeBatch(){
    	Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
		this.sqlgGraph.tx().batchMode(BatchModeType.NORMAL);
    	Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
    	this.sqlgGraph.tx().commit();
    	this.sqlgGraph.tx().batchMode(BatchModeType.NORMAL);
    	assertNotNull(a1.id());
    	Vertex a2=this.sqlgGraph.traversal().V(a1.id()).next();
    	assertNotSame(a1, a2);
    	assertEquals(a1,a2);
    	assertEquals(a1.hashCode(),a2.hashCode());
    }
}
