package org.umlg.sqlg.test.properties;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

import static org.junit.Assert.*;

/**
 * test behavior on property values
 *
 * @author JP Moresmau
 */
public class TestPropertyValues extends BaseTest {


    @Test
    public void testMultipleSelect() {
		Map<String, Object> aValues = new HashMap<>();
		aValues.put("name", "root");
		Vertex vA = sqlgGraph.addVertex("A", aValues);
		Map<String, Object> iValues = new HashMap<>();
		iValues.put("name", "item1");
		Vertex vI = sqlgGraph.addVertex("I", iValues);
		vA.addEdge("likes", vI, "howMuch", 5, "who", "Joe");
		this.sqlgGraph.tx().commit();
		Object id0 = vI.id();
        GraphTraversal<Vertex, Map<String, Object>> gt = sqlgGraph.traversal().V()
                .hasLabel("A")
                .has("name", "root")
                .outE("likes")
                .as("e")
                .values("howMuch").as("stars")
                .select("e")
                .values("who").as("user")
                .select("e")
                .inV()
                .id().as("item")
                .select("user", "stars", "item");
        printTraversalForm(gt);
        assertTrue(gt.hasNext());
        Map<String, Object> m = gt.next();
        assertEquals(new Integer(5), m.get("stars"));
        assertEquals("Joe", m.get("user"));
        assertEquals(id0, m.get("item"));
    }

	/**
	 * If the order() does not happen on the database, i.e. in java code then the property also needs to be present.
	 */
	@Test
	public void testInMemoryOrderByValues(){
		loadModern();
		final Traversal<Vertex, String> traversal =  this.sqlgGraph.traversal().V().both().hasLabel("person").order().by("age", Order.decr).limit(5).values("name");
		printTraversalForm(traversal);
		checkOrderedResults(Arrays.asList("peter", "josh", "josh", "josh", "marko"), traversal);
	}

	public static <T> void checkOrderedResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
		final List<T> results = traversal.toList();
		assertFalse(traversal.hasNext());
		if (expectedResults.size() != results.size()) {
			assertEquals("Checking result size", expectedResults.size(), results.size());
		}
		for (int i = 0; i < expectedResults.size(); i++) {
			assertEquals(expectedResults.get(i), results.get(i));
		}
	}

    @Test
    public void testValueMapOneObject() {
        loadModern();
        final Traversal<Vertex, Map<String, Object>> traversal = sqlgGraph.traversal().V().hasLabel("person").valueMap("name");
        printTraversalForm(traversal);
        checkColumnsNotPresent(traversal, "age");
        checkRestrictedProperties(traversal, "name");
        Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            Map<String, Object> m = traversal.next();
            assertNotNull(m);
            assertEquals(1, m.size());
            assertTrue(m.containsKey("name"));
            Object v = m.get("name");
            // "It is important to note that the map of a vertex maintains a list of values for each key."
            assertTrue(v instanceof List<?>);
            List<?> l = (List<?>) v;
            assertEquals(1, l.size());
            Object v1 = l.get(0);
            assertTrue(v1 instanceof String);
            names.add((String) v1);
        }
        assertEquals(new HashSet<>(Arrays.asList("marko", "vadas", "josh", "peter")), names);
    }
    	@Test
	public void testValueMapAllObject(){
		loadModern();
		final Traversal<Vertex, Map<String,Object>> traversal = sqlgGraph.traversal().V().hasLabel("person").valueMap();
		printTraversalForm(traversal);
		checkNoRestrictedProperties(traversal);
    	Set<String> names=new HashSet<>();
    	Set<Integer> ages=new HashSet<>();
    	while (traversal.hasNext()){
    		Map<String,Object> m=traversal.next();
    		assertNotNull(m);
    		assertEquals(2, m.size());
    		assertTrue(m.containsKey("name"));
    		Object v=m.get("name");
    		// "It is important to note that the map of a vertex maintains a list of values for each key."
    		assertTrue(v instanceof List<?>);
    		List<?> l=(List<?>)v;
    		assertEquals(1,l.size());
    		Object v1=l.get(0);
    		assertTrue(v1 instanceof String);
    		names.add((String)v1);
    		assertTrue(m.containsKey("age"));
    		v=m.get("age");
    		// "It is important to note that the map of a vertex maintains a list of values for each key."
    		assertTrue(v instanceof List<?>);
    		l=(List<?>)v;
    		assertEquals(1,l.size());
    		v1=l.get(0);
    		assertTrue(v1 instanceof Integer);
    		ages.add((Integer)v1);
    	}
    	assertEquals(new HashSet<>(Arrays.asList("marko","vadas","josh","peter")),names);
    	assertEquals(new HashSet<>(Arrays.asList(29,27,32,35)),ages);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testValueMapAliasVertex(){
		loadModern();
		final Traversal<Vertex, Map<String,Object>> traversal = sqlgGraph.traversal()
                .V().hasLabel("person").as("a")
                .valueMap("name").as("b")
                .select("a","b");
		printTraversalForm(traversal);
		checkNoRestrictedProperties(traversal);
		Set<String> names1=new HashSet<>();
		Set<String> names2=new HashSet<>();
		Set<Integer> ages=new HashSet<>();
		while (traversal.hasNext()){
    		Map<String,Object> m=traversal.next();
    		assertNotNull(m);
    		assertEquals(2, m.size());
    		assertTrue(m.containsKey("a"));
    		assertTrue(m.containsKey("b"));
    		Vertex v=(Vertex)m.get("a");
    		assertTrue(v.property("name").isPresent());
    		assertTrue(v.property("age").isPresent());
    		names1.add((String)v.property("name").value());
    		ages.add((Integer)v.property("age").value());

    		Map<String,Object> m2=(Map<String,Object>)m.get("b");
    		Object o=m2.get("name");
    		// "It is important to note that the map of a vertex maintains a list of values for each key."
    		assertTrue(o instanceof List<?>);
    		List<?> l=(List<?>)o;
    		assertEquals(1,l.size());
    		Object v1=l.get(0);
    		assertTrue(v1 instanceof String);
    		names2.add((String)v1);
		}
		assertEquals(names1,names2);
		assertEquals(new HashSet<>(Arrays.asList("marko","vadas","josh","peter")),names1);
		assertEquals(new HashSet<>(Arrays.asList(29,27,32,35)),ages);
	}

	@Test
	public void testValueMapAlias(){
		loadModern();
		final Traversal<Vertex, Map<String,Object>> traversal = sqlgGraph.traversal().V().hasLabel("person").valueMap("name").as("b").select("b");
		checkColumnsNotPresent(traversal,"age");
		checkRestrictedProperties(traversal,"name");
		Set<String> names=new HashSet<>();
		while (traversal.hasNext()){
    		Map<String,Object> m=traversal.next();
    		assertNotNull(m);
    		assertEquals(1, m.size());
    		Object o=m.get("name");
    		// "It is important to note that the map of a vertex maintains a list of values for each key."
    		assertTrue(o instanceof List<?>);
    		List<?> l=(List<?>)o;
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
		checkColumnsNotPresent(traversal,"age");
		checkRestrictedProperties(traversal,"name");
    	Set<String> names=new HashSet<>();
    	while (traversal.hasNext()){
    		names.add(traversal.next());
    	}
    	assertEquals(new HashSet<>(Arrays.asList("marko","vadas","josh","peter")),names);
	}

	@Test
	public void testValuesAll(){
		loadModern();
		final Traversal<Vertex, Object> traversal = sqlgGraph.traversal().V().hasLabel("person").values();
		printTraversalForm(traversal);
		checkNoRestrictedProperties(traversal);
    	Set<Object> values=new HashSet<>();
    	while (traversal.hasNext()){
    		values.add(traversal.next());
    	}
    	assertEquals(new HashSet<>(Arrays.asList("marko","vadas","josh","peter",29,27,32,35)),values);
	}


	@Test
	public void testValuesOneWhere(){
		loadModern();
		final Traversal<Vertex, String> traversal = sqlgGraph.traversal().V().hasLabel("person").has("age",29).values("name");
		checkColumnsNotPresent(traversal,"age");
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
        // name because explicitly requested, age because we order on it
        checkRestrictedProperties(traversal,"name","age");
        assertTrue(traversal.hasNext());
        assertEquals(Arrays.asList("marko", "josh", "peter"), traversal.toList());
    }

	@Test
	public void testOut(){
		loadModern();

        final Traversal<Vertex, String> traversal =  this.sqlgGraph.traversal()
                .V().hasLabel("person")
                .out("created")
                .values("name");
        checkColumnsNotPresent(traversal,"language");
        checkRestrictedProperties(traversal,"name");
    	Set<String> names=new HashSet<>();
    	while (traversal.hasNext()){
    		names.add(traversal.next());
    	}
    	assertEquals(new HashSet<>(Arrays.asList("lop","ripple")),names);
	}

    @Test
    public void g_V_both_name_order_byXa_bX_dedup_value() {
        loadModern();
        @SuppressWarnings("ComparatorCombinators")
        final Traversal<Vertex, String> traversal =  this.sqlgGraph.traversal()
                .V()
                .both()
                .<String>properties("name")
                .order().by((a, b) -> a.value().compareTo(b.value()))
                .dedup().value();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(6, names.size());
        assertEquals("josh", names.get(0));
        assertEquals("lop", names.get(1));
        assertEquals("marko", names.get(2));
        assertEquals("peter", names.get(3));
        assertEquals("ripple", names.get(4));
        assertEquals("vadas", names.get(5));
        assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_valuesXnameX_order_tail() {
        loadModern();
        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal().V().<String>values("name").order().tail();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList("vadas"), traversal.toList());
    }

    /**
     * check provided columns/properties are not selected in the SQL
     *
     * @param t          the traversal
     * @param properties the properties to check for absence
     */
    private void checkColumnsNotPresent(Traversal<?, ?> t, String... properties) {
        String sql = getSQL(t);
        assertNotNull(sql);
        sql = sql.trim();
        assertTrue(sql.startsWith("SELECT"));
        int ix = sql.indexOf("FROM");
        assertTrue(ix > 0);
        String select = sql.substring(0, ix);
        for (String p : properties) {
            assertFalse(select.contains(p));
        }
    }

    /**
     * check the replaced steps has the specified restricted properties
     *
     * @param t          the traversal
     * @param properties the properties
     */
    @SuppressWarnings({"resource", "unchecked"})
    private void checkRestrictedProperties(Traversal<?, ?> t, String... properties) {
        boolean found = false;
        for (Step<?, ?> s : ((Admin<?, ?>) t).getSteps()) {
            if (s instanceof SqlgGraphStep) {
                SqlgGraphStep<?, SqlgElement> gs = (SqlgGraphStep<?, SqlgElement>) s;
                ReplacedStep<?, ?> rs = gs.getReplacedSteps().get(gs.getReplacedSteps().size() - 1);
                assertEquals(new HashSet<>(Arrays.asList(properties)), rs.getRestrictedProperties());
                found = true;
            }
        }
        assertTrue(found);
    }

    /**
     * check the replaced steps has the specified restricted properties
     *
     * @param t the traversal, EVALUATED (ie call printTraversalForm or getSQL first)
     */
    @SuppressWarnings({"resource", "unchecked"})
    private void checkNoRestrictedProperties(Traversal<?, ?> t) {
        boolean found = false;
        for (Step<?, ?> s : ((Admin<?, ?>) t).getSteps()) {
            if (s instanceof SqlgGraphStep) {
                SqlgGraphStep<?, SqlgElement> gs = (SqlgGraphStep<?, SqlgElement>) s;
                ReplacedStep<?, ?> rs = gs.getReplacedSteps().get(gs.getReplacedSteps().size() - 1);
                assertNull(String.valueOf(rs.getRestrictedProperties()), rs.getRestrictedProperties());
                found = true;
            }
        }
        assertTrue(found);
    }

}
