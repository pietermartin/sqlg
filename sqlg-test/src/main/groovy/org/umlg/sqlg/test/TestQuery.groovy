package org.umlg.sqlg.test

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Assume
import org.junit.Test

import static org.junit.Assert.*

import org.apache.tinkerpop.gremlin.process.T;

/**
 * Created by pieter on 2014/07/31.
 */
class TestQuery extends BaseTest {

    @Test
    public void testQuery() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStringArrayValues());
        String[] arrStr = ['Ananas', 'Banana', 'Kiwi']
        this.sqlgGraph.addVertex("Person", [name     : "Pieter", ageI: 40, ageL: 40L, ageF: 40F, ageD: 40D, ageS: (short) 1,
                                            nameArray: arrStr]);
        this.sqlgGraph.addVertex("Person", [name     : "Marko", ageI: 40, ageL: 40L, ageF: 40F, ageD: 40D, ageS: (short) 1,
                                            nameArray: arrStr]);
        this.sqlgGraph.tx().commit()
        assertEquals(2, this.sqlgGraph.V().count().next());
        String json = this.sqlgGraph.query("select * from \"V_Person\"");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonAsMap = mapper.readValue(json, Map.class);
        List<Map<String, Object>> data = jsonAsMap.get("data");
        List<Map<String, Object>> meta = jsonAsMap.get("meta");
        assertEquals(2, data.size());
        assertEquals("Pieter", data.get(0).get("name"));
        assertEquals(40, data.get(0).get("ageI"));
        assertEquals(40L, data.get(0).get("ageL"));
        assertEquals(40D, data.get(0).get("ageF"), 0);
        assertEquals(40D, data.get(0).get("ageD"), 0);
        assertEquals((short) 1, data.get(0).get("ageS"));
        assertNotNull(data.get(0).get("nameArray"));
        assertTrue(data.get(0).get("nameArray") instanceof List);
        assertTrue(((List<String>) data.get(0).get("nameArray")).contains('Ananas'));
    }

}
