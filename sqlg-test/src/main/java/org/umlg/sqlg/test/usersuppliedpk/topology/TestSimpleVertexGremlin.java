package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/03/18
 */
public class TestSimpleVertexGremlin extends BaseTest {

    @Test
    public void testSimpleVertexInsertAndUpdateAndQuery() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        this.sqlgGraph.tx().commit();

        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertTrue(persons.isEmpty());
        try {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
            Assert.fail("Primary Key not specified, query suppose to fail!");
        } catch (Exception  e) {
            //ignore
        }
        this.sqlgGraph.tx().rollback();

        this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        this.sqlgGraph.tx().commit();
        persons = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(1, persons.size());
        Vertex person = persons.get(0);
        RecordId recordId = (RecordId) person.id();
        Assert.assertTrue(recordId.getId() == null);
        Assert.assertEquals(2, recordId.getIdentifiers().size());
        Assert.assertEquals("John", recordId.getIdentifiers().get(0));
        Assert.assertEquals("Smith", recordId.getIdentifiers().get(1));
        Assert.assertEquals("John", person.property("name").value());
        Assert.assertEquals("Smith", person.property("surname").value());
        Assert.assertFalse(person.property("country").isPresent());

        person.property("country", "moon");
        this.sqlgGraph.tx().commit();
        person = this.sqlgGraph.traversal().V().hasLabel("Person").toList().get(0);
        Assert.assertTrue(person.property("country").isPresent());
        Assert.assertEquals("moon", person.value("country"));
    }


}
