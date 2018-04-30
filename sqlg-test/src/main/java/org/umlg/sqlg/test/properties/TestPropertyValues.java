package org.umlg.sqlg.test.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.test.BaseTest;

/**
 * test behavior on property values
 * @author JP Moresmau
 *
 */
public class TestPropertyValues extends BaseTest {

	@Test
	public void testValueMapOneObject(){
		loadModern();
		final Traversal<Vertex, Map<String,Object>> traversal = sqlgGraph.traversal().V().hasLabel("person").valueMap("name");
		printTraversalForm(traversal);
		checkRestrictedProperties(traversal,"name");
    	Set<String> names=new HashSet<>();
    	while (traversal.hasNext()){
    		Map<String,Object> m=traversal.next();
    		assertNotNull(m);
    		assertEquals(1, m.size());
    		assertTrue(m.containsKey("name"));
    		Object v=m.get("name");
    		// "It is important to note that the map of a vertex maintains a list of values for each key."
    		assertTrue(v instanceof List<?>);
    		List<?> l=(List<?>)v;
    		assertEquals(1,l.size());
    		Object v1=l.get(0);
    		assertTrue(v1 instanceof String);
    		names.add((String)v1);
    	}
    	assertEquals(new HashSet<>(Arrays.asList("marko","vadas","josh","peter")),names);
	}
	
	
	@Test
	public void testValuesOne(){
		loadModern();
		final Traversal<Vertex, String> traversal = sqlgGraph.traversal().V().hasLabel("person").values("name");
		printTraversalForm(traversal);
		checkRestrictedProperties(traversal,"name");
    	Set<String> names=new HashSet<>();
    	while (traversal.hasNext()){
    		names.add(traversal.next());
    	}
    	assertEquals(new HashSet<>(Arrays.asList("marko","vadas","josh","peter")),names);
	}
	
	@Test
	public void testValuesOneWhere(){
		loadModern();
		final Traversal<Vertex, String> traversal = sqlgGraph.traversal().V().hasLabel("person").has("age",29).values("name");
		printTraversalForm(traversal);
		checkRestrictedProperties(traversal,"name");
    	Set<String> names=new HashSet<>();
    	while (traversal.hasNext()){
    		names.add(traversal.next());
    	}
    	assertEquals(new HashSet<>(Arrays.asList("marko")),names);
	}
	 
	@Test
    public void g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX() {
        loadModern();

        final Traversal<Vertex, String> traversal =  this.sqlgGraph.traversal()
                .V().hasLabel("person")
                .order().by("age").skip(1).values("name");
        printTraversalForm(traversal);
        checkRestrictedProperties(traversal,"name");
        assertTrue(traversal.hasNext());
        assertEquals(Arrays.asList("marko", "josh", "peter"), traversal.toList());
    }
	
	@SuppressWarnings({ "resource", "unchecked" })
	private void checkRestrictedProperties(Traversal<?, ?> t, String... properties){
		boolean found=false;
		for (Step<?, ?> s:((Admin<?, ?>)t).getSteps()){
			if (s instanceof SqlgGraphStep){
				SqlgGraphStep<?,SqlgElement> gs=(SqlgGraphStep<?, SqlgElement>)s;
				ReplacedStep<?, ?> rs=gs.getReplacedSteps().get(gs.getReplacedSteps().size()-1);
				assertEquals(new HashSet<>(Arrays.asList(properties)),rs.getRestrictedProperties());
				found=true;
			}
		}
		assertTrue(found);
	}
}
