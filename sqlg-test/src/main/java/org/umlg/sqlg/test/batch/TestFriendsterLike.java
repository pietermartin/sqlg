package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.test.BaseTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/15
 * Time: 3:50 PM
 */
public class TestFriendsterLike extends BaseTest {

//    @Test
    public void testSeparateThread() {
        List<String> lines = new ArrayList<>();
        lines.add("1|2,3,4,5,6,7,8,9,10");
        lines.add("2|1,3,4,5,6,7,8,9,10");
        lines.add("3|1,2,4,5,6,7,8,9,10");
        lines.add("4|1,2,3,5,6,7,8,9,10");
        lines.add("5|1,2,3,4,6,7,8,9,10");
        lines.add("6|1,2,3,4,5,7,8,9,10");
        lines.add("7|1,2,3,4,5,6,8,9,10");
        lines.add("8|1,2,3,4,5,6,7,9,10");
        lines.add("9|1,2,3,4,5,6,7,8,10");
        lines.add("10|1,2,3,4,5,6,7,8,9");
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (String line : lines) {
            String[] parts = line.split("\\|");
            String id = parts[0];
            this.sqlgGraph.streamVertex(T.label, "Person", "index", id);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        List<Pair<String, String>> uids = new ArrayList<>();
        for (String line : lines) {
            String[] parts = line.split("\\|");
            String id = parts[0];
            String friends = parts[1];
            String[] friendIds = friends.split(",");
            for (String friendId : friendIds) {
                uids.add(Pair.of(id, friendId));
            }
        }
        this.sqlgGraph.bulkAddEdges("Person", "Person", "friend", Pair.of("index", "index"), uids);
        this.sqlgGraph.tx().commit();

        assertEquals(10, this.sqlgGraph.traversal().V().count().next(), 0);
        for (int i = 1; i < 11; i++) {
            assertEquals(9, this.sqlgGraph.traversal().V(RecordId.from(SchemaTable.of("public", "Person"), Long.valueOf(i))).out().count().next(), 0);
        }
        System.out.println(this.sqlgGraph.traversal().V().both().both().count().next());
    }

//    @Test
    public void testFriendsterLoad() throws IOException {
//        String dir = "/home/pieter/Downloads/friendster/friendster-dataset-201107/";
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        String dir = "/home/pieter/Downloads/friendster/friendsterSmall/";
        String NOT_FOUND = "notfound";
        String PRIVATE = "private";
        int count = 1;
        this.sqlgGraph.tx().streamingBatchModeOn();
        try (DirectoryStream<java.nio.file.Path> directoryStream = Files.newDirectoryStream(Paths.get(dir))) {
            for (java.nio.file.Path path : directoryStream) {
                BufferedReader bufferedReader = Files.newBufferedReader(path);
                bufferedReader.lines().forEach(line -> {

                    String[] parts = line.split(":");
                    String id = parts[0];
                    String last;
                    if (parts.length > 1) {
                        last = parts[1];
                    } else {
                        last = "-";
                    }
                    if (last.equals(NOT_FOUND) || last.equals(PRIVATE)) {
                        this.sqlgGraph.streamVertex(T.label, "Person", "index", id, "friend", last);
                    } else {
                        this.sqlgGraph.streamVertex(T.label, "Person", "index", id, "friend", "_");
                    }

                });
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().streamingBatchModeOn();
                System.out.println("Vertex Load " + count++);
            }
        }
        System.out.println("Done Vertex Loading");

        count = 1;
        try (DirectoryStream<java.nio.file.Path> directoryStream = Files.newDirectoryStream(Paths.get(dir))) {
            for (java.nio.file.Path path : directoryStream) {
                List<Pair<String, String>> uids = new ArrayList<>();
                BufferedReader bufferedReader = Files.newBufferedReader(path);
                bufferedReader.lines().forEach(line -> {

                    String[] parts = line.split(":");
                    String id = parts[0];
                    if (parts.length > 1) {
                        String friends = parts[1];
                        if (!friends.equals(NOT_FOUND) && !friends.equals(PRIVATE)) {
                            String[] friendIds = friends.split(",");
                            for (String friendId : friendIds) {
                                uids.add(Pair.of(id, friendId));
                            }
                        }
                    }

                });
                this.sqlgGraph.bulkAddEdges("Person", "Person", "friend", Pair.of("index", "index"), uids);
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().streamingBatchModeOn();
                System.out.println("Edge Load " + count++);
            }
        }
        System.out.println("Done Edge Loading");
        stopWatch.stop();
        System.out.println("Time taken = " + stopWatch.toString());
    }
}
