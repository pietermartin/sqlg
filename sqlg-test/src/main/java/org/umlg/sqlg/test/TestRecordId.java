package org.umlg.sqlg.test;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.topology.Schema;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/04/30
 */
public class TestRecordId extends BaseTest {

    @Test
    public void testRecordIdHasCommas() {
        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        schema.ensureVertexLabelExist("aaa.bbb", new LinkedHashMap<>() {{
            put("_id", PropertyType.varChar(100));
            put("value", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("_id")));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "A.aaa.bbb", "_id", "id1", "value", "a");
        this.sqlgGraph.tx().commit();

        RecordId recordId = RecordId.from(this.sqlgGraph, "A.aaa.bbb:::[id1]");
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A.aaa.bbb").hasId(P.eq(recordId)).count().next(), 0L);

        schema.ensureVertexLabelExist("aaa.ccc", new LinkedHashMap<>() {{
            put("_id", PropertyType.varChar(100));
            put("value", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("_id")));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "A.aaa.ccc", "_id", "id:::what", "value", "a");
        this.sqlgGraph.tx().commit();

        recordId = RecordId.from(this.sqlgGraph, "A.aaa.ccc:::[id:::what]");
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A.aaa.ccc").hasId(P.eq(recordId)).count().next(), 0L);
    }

    @Test
    public void testTableHasPeriod() {
        this.sqlgGraph.addVertex("A.aaa.bbb");
        this.sqlgGraph.tx().commit();
        String id = "A.aaa.bbb" + RecordId.RECORD_ID_DELIMITER + this.sqlgGraph.getSqlDialect().getPrimaryKeyStartValue();
        RecordId recordId = RecordId.from(id);
        Assert.assertEquals(
                SchemaTable.of("A", "aaa.bbb"),
                recordId.getSchemaTable()
        );
        Assert.assertEquals(
                this.sqlgGraph.getSqlDialect().getPrimaryKeyStartValue(),
                recordId.sequenceId()
        );
    }

    @Test
    public void testRecordIdFromElement() {
        this.sqlgGraph.addVertex("A");
        this.sqlgGraph.tx().commit();
        RecordId recordId = RecordId.from(this.sqlgGraph.getTopology().getPublicSchema().getName() + ".A" + RecordId.RECORD_ID_DELIMITER + this.sqlgGraph.getSqlDialect().getPrimaryKeyStartValue());
        Assert.assertEquals(
                SchemaTable.of(this.sqlgGraph.getTopology().getPublicSchema().getName(), "A"),
                recordId.getSchemaTable()
        );
        Assert.assertEquals(
                this.sqlgGraph.getSqlDialect().getPrimaryKeyStartValue(),
                recordId.sequenceId()
        );
    }

    @Test
    public void testRecordIdFromElementUserSuppliedPK() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "A",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Collections.singletonList("uid1")));
        String uid1 = UUID.randomUUID().toString();
        this.sqlgGraph.addVertex(T.label, "A", "uid1", uid1);
        this.sqlgGraph.tx().commit();

        RecordId recordId = RecordId.from(this.sqlgGraph, this.sqlgGraph.getTopology().getPublicSchema().getName() + ".A" + RecordId.RECORD_ID_DELIMITER + "[" + uid1 + "]");
        Assert.assertEquals(
                SchemaTable.of(this.sqlgGraph.getTopology().getPublicSchema().getName(), "A"),
                recordId.getSchemaTable()
        );
        Assert.assertEquals(1, recordId.getIdentifiers().size());
        Assert.assertEquals(uid1, recordId.getIdentifiers().get(0));
    }

    @Test
    public void testRecordIdFromElementUserSuppliedPK_With2Ids() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "A",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2")));
        String uid1 = UUID.randomUUID().toString();
        String uid2 = UUID.randomUUID().toString();
        this.sqlgGraph.addVertex(T.label, "A", "uid1", uid1, "uid2", uid2);
        this.sqlgGraph.tx().commit();

        RecordId recordId = RecordId.from(this.sqlgGraph, this.sqlgGraph.getTopology().getPublicSchema().getName() + ".A" + RecordId.RECORD_ID_DELIMITER + "[" + uid1 + ", " + uid2 + "]");
        Assert.assertEquals(
                SchemaTable.of(this.sqlgGraph.getTopology().getPublicSchema().getName(), "A"),
                recordId.getSchemaTable()
        );
        Assert.assertEquals(2, recordId.getIdentifiers().size());
        Assert.assertEquals(uid1, recordId.getIdentifiers().get(0));
        Assert.assertEquals(uid2, recordId.getIdentifiers().get(1));
    }

    @Test
    public void testRecordIdFromElementUserSuppliedPK_With3Ids() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "A",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("uid3", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "uid3")));
        String uid1 = UUID.randomUUID().toString();
        String uid2 = UUID.randomUUID().toString();
        String uid3 = UUID.randomUUID().toString();
        this.sqlgGraph.addVertex(T.label, "A", "uid1", uid1, "uid2", uid2, "uid3", uid3);
        this.sqlgGraph.tx().commit();

        RecordId recordId = RecordId.from(this.sqlgGraph, this.sqlgGraph.getTopology().getPublicSchema().getName() + ".A" + RecordId.RECORD_ID_DELIMITER + "[" + uid1 + ", " + uid2 + ", " + uid3 + "]");
        Assert.assertEquals(
                SchemaTable.of(this.sqlgGraph.getTopology().getPublicSchema().getName(), "A"),
                recordId.getSchemaTable()
        );
        Assert.assertEquals(3, recordId.getIdentifiers().size());
        Assert.assertEquals(uid1, recordId.getIdentifiers().get(0));
        Assert.assertEquals(uid2, recordId.getIdentifiers().get(1));
        Assert.assertEquals(uid3, recordId.getIdentifiers().get(2));
    }
}
