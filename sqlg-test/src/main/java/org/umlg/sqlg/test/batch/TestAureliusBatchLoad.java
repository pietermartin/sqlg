package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.BatchManager;
import org.umlg.sqlg.test.BaseTest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Date: 2016/05/11
 * Time: 10:31 PM
 */
public class TestAureliusBatchLoad extends BaseTest {

    @Test
    public void test() {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.STREAMING);
        String line = "";
        String fileName = "/home/pieter/Downloads/edges_bukl.txt";
        String edgesFile = "/home/pieter/Downloads/edges_api.txt";
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split("\\|");
                String vertexId = parts[0];
                this.sqlgGraph.streamVertex("index", vertexId);
            }
            bufferedReader.close();
            this.sqlgGraph.tx().commit();

            this.sqlgGraph.tx().normalBatchModeOn();
            fileReader = new FileReader(edgesFile);
            bufferedReader = new BufferedReader(fileReader);
            Set<String> indexes = new HashSet<>();
            List<Pair<String, String>> edges = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split(",");
                String inVertex = parts[0];
                String outVertex = parts[1];
                indexes.add(inVertex);
                indexes.add(outVertex);
                edges.add(Pair.of(inVertex, outVertex));
            }
            Map<String, Vertex> indexMap = this.sqlgGraph.traversal().V()
                    .has("index", P.within(indexes))
                    .toStream().collect(Collectors.toMap((v1)->v1.<String>value("index"), Function.identity()));
//            Assert.assertEquals(2, vertexTraversal(a1).out().has("name", "b2").has("name", "b2").out().has("name", P.within(Arrays.asList("c5", "c6"))).count().next().intValue());
            for (Pair<String, String> edge : edges) {
                Vertex in = indexMap.get(edge.getLeft());
                Vertex out = indexMap.get(edge.getRight());
                if (in != null && out != null) {
                    in.addEdge("link", out);
                } else {
                    System.out.println("found null " + edge.toString());
                }
            }
//            this.sqlgGraph.bulkAddEdges("vertex", "vertex", "link", Pair.of("index", "index"), uids);
            this.sqlgGraph.tx().commit();
            // Always close files.
            bufferedReader.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");
        } catch (IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");
        }
    }
}
