package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pieter on 2015/09/28.
 */
public class TestServerSideEdgeCreation  extends BaseTest {

//    select (select a."ID" from public."V_A" a where "index" = 0) as "a", b."ID" from public."V_B" b where "index" in (0,1,2,3,4,5,6,7,8,9)
    @Test
    public void test() {

        List<Pair<String,String>> uids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String iAsString = Integer.toString(i);
            this.sqlgGraph.addVertex(T.label, "A", "index", iAsString);
            for (int j = 0; j < 100; j++) {
                String ijAsString = Integer.toString(i) + Integer.toString(j);
                this.sqlgGraph.addVertex(T.label, "B", "index", ijAsString);
                uids.add(Pair.of(iAsString, ijAsString));
            }
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchMode();
        SchemaTable a = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        SchemaTable b = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "B");
        SchemaTable ab = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "AB");
        this.sqlgGraph.bulkAddEdges(a, b, ab, uids);
        this.sqlgGraph.tx().commit();
    }

}
