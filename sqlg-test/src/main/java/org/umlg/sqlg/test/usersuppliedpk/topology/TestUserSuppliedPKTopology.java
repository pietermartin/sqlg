package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/03/17
 */
public class TestUserSuppliedPKTopology extends BaseTest {

    @SuppressWarnings("Duplicates")
    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            if (isPostgres()) {
                configuration.addProperty(SqlgGraph.DISTRIBUTED, true);
                if (!configuration.containsKey(SqlgGraph.JDBC_URL))
                    throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", SqlgGraph.JDBC_URL));
            }
        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testVertexCompositeIds() throws Exception {
        this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );

        Optional<VertexLabel> personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
        Assert.assertTrue(personVertexLabel.isPresent());
        Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
        Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
        Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));

        //HSQLDB does not support transactional schema creation.
        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            this.sqlgGraph.tx().rollback();
            personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
            Assert.assertFalse(personVertexLabel.isPresent());
            if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
                personVertexLabel = this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("Person");
                Assert.assertFalse(personVertexLabel.isPresent());
            }
        }

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
        Thread.sleep(2000);

        personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
        Assert.assertTrue(personVertexLabel.isPresent());
        Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
        Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
        Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));

        if (isPostgres()) {
            personVertexLabel = this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("Person");
            Assert.assertTrue(personVertexLabel.isPresent());
            Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
            Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
            Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));
        }

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        personVertexLabel = sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
        Assert.assertTrue(personVertexLabel.isPresent());
        Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
        Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
        Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));

        dropSqlgSchema(sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            personVertexLabel = sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
            Assert.assertTrue(personVertexLabel.isPresent());
            Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
            Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
            Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));
        }
    }

    @Test
    public void testEdgeCompositeIds() throws Exception {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        VertexLabel addressVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new LinkedHashMap<String, PropertyType>() {{
                    put("street", PropertyType.STRING);
                    put("suburb", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                    put("province", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("street", "suburb"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "livesAt",
                personVertexLabel,
                addressVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.STRING);
                    put("uid2", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2")));

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            this.sqlgGraph.tx().rollback();
            Optional<EdgeLabel> livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertFalse(livesAt.isPresent());
            if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
                livesAt = this.sqlgGraph1.getTopology().getEdgeLabel(this.sqlgGraph1.getSqlDialect().getPublicSchema(), "livesAt");
                Assert.assertFalse(livesAt.isPresent());
            }
        }

        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "livesAt",
                personVertexLabel,
                addressVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.STRING);
                    put("uid2", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2")));
        this.sqlgGraph.tx().commit();

        Thread.sleep(1000);

        Optional<EdgeLabel> livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());
        Assert.assertEquals(2, livesAt.get().getIdentifiers().size());
        Assert.assertEquals("uid1", livesAt.get().getIdentifiers().get(0));
        Assert.assertEquals("uid2", livesAt.get().getIdentifiers().get(1));

        if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
            livesAt = this.sqlgGraph1.getTopology().getEdgeLabel(this.sqlgGraph1.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertTrue(livesAt.isPresent());
            Assert.assertEquals(2, livesAt.get().getIdentifiers().size());
            Assert.assertEquals("uid1", livesAt.get().getIdentifiers().get(0));
            Assert.assertEquals("uid2", livesAt.get().getIdentifiers().get(1));
        }

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());
        Assert.assertEquals(2, livesAt.get().getIdentifiers().size());
        Assert.assertEquals("uid1", livesAt.get().getIdentifiers().get(0));
        Assert.assertEquals("uid2", livesAt.get().getIdentifiers().get(1));

        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        this.sqlgGraph = SqlgGraph.open(configuration);
        livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());
        Assert.assertEquals(2, livesAt.get().getIdentifiers().size());
        Assert.assertEquals("uid1", livesAt.get().getIdentifiers().get(0));
        Assert.assertEquals("uid2", livesAt.get().getIdentifiers().get(1));
    }

    @Test
    public void testEdgeNormal() throws Exception {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }});
        VertexLabel addressVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new LinkedHashMap<String, PropertyType>() {{
                    put("street", PropertyType.STRING);
                    put("suburb", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                    put("province", PropertyType.STRING);
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "livesAt",
                personVertexLabel,
                addressVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.STRING);
                    put("uid2", PropertyType.STRING);
                }});
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            Optional<EdgeLabel> livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertFalse(livesAt.isPresent());
        }
        if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
            Optional<EdgeLabel> livesAt = this.sqlgGraph1.getTopology().getEdgeLabel(this.sqlgGraph1.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertFalse(livesAt.isPresent());
        }

        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "livesAt",
                personVertexLabel,
                addressVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.STRING);
                    put("uid2", PropertyType.STRING);
                }});
        this.sqlgGraph.tx().commit();

        Thread.sleep(1000);

        Optional<EdgeLabel> livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());

        if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
            livesAt = this.sqlgGraph1.getTopology().getEdgeLabel(this.sqlgGraph1.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertTrue(livesAt.isPresent());
        }

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());

        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        this.sqlgGraph = SqlgGraph.open(configuration);
        livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());
    }
}
