package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.umlg.sqlg.structure.topology.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/02/01
 */
class PartitionTree {

    private final String schema;
    private final String name;
    /**
     * partitionExpression1 represents the partition columns.
     */
    private final String partitionExpression1;
    /**
     * partitionExpression2 is if the partition expression is a sql expression. i.e. not just specifying columns.
     */
    private final String partitionExpression2;
    private final String fromToInModulusRemainder;
    private final PartitionType partitionType;

    private String parent;
    private final String schemaAndName;
    private PartitionTree parentPartitionTree;
    private final List<PartitionTree> children = new ArrayList<>();
    private static final Map<String, PartitionTree> flattenedPartitionTrees = new HashMap<>();

    static synchronized List<PartitionTree> build(List<Map<String, String>> partitions) {
        flattenedPartitionTrees.clear();
        List<PartitionTree> result = new ArrayList<>();
        //First add all partitions that is a child
        for (Map<String, String> partition : partitions) {
            if (partition.get("parent") != null) {
                PartitionTree p = new PartitionTree(
                        partition.get("schema") + "." + partition.get("child"),
                        partition.get("schema"),
                        partition.get("child"),
                        partition.get("partitionType"),
                        partition.get("partitionExpression1"),
                        partition.get("partitionExpression2"),
                        partition.get("fromToInModulusRemainder")
                );
                p.parent = partition.get("schema") + "." + partition.get("parent");
                flattenedPartitionTrees.put(p.schemaAndName, p);
            }
        }
        //Now add the root partition, i.e. that is not also a child
        for (Map<String, String> partition : partitions) {
            if (partition.get("parent") == null) {
                PartitionTree p = new PartitionTree(
                        partition.get("schema") + "." + partition.get("child"),
                        partition.get("schema"),
                        partition.get("child"),
                        partition.get("partitionType"),
                        partition.get("partitionExpression1"),
                        partition.get("partitionExpression2"),
                        partition.get("fromToInModulusRemainder")
                );
                if (!flattenedPartitionTrees.containsKey(p.schemaAndName)) {
                    flattenedPartitionTrees.put(p.schemaAndName, p);
                    result.add(p);
                }
            }
        }
        for (PartitionTree partitionTree : flattenedPartitionTrees.values()) {
            if (partitionTree.parent != null) {
                PartitionTree parentPartitionTree = flattenedPartitionTrees.get(partitionTree.parent);
                parentPartitionTree.addChild(partitionTree);
            }
        }
        return result;
    }

    private void addChild(PartitionTree partitionTree) {
        this.children.add(partitionTree);
        partitionTree.parentPartitionTree = this;
    }

    private PartitionTree(
            String schemaAndName,
            String schema,
            String name,
            String partitionType,
            String partitionExpression1,
            String partitionExpression2,
            String fromToInModulusRemainder) {

        this.schemaAndName = schemaAndName;
        this.schema = schema;
        this.name = StringUtils.removeEnd(StringUtils.removeFirst(name, "\""), "\"");
        this.partitionExpression1 = partitionExpression1;
        this.partitionExpression2 = partitionExpression2;
        this.fromToInModulusRemainder = fromToInModulusRemainder;
        if (partitionType == null) {
            this.partitionType = PartitionType.NONE;
        } else {
            this.partitionType = PartitionType.fromPostgresPartStrat(partitionType);
        }
    }

    public void createPartitions(SqlgGraph sqlgGraph) {
        Preconditions.checkState(this.name.startsWith(Topology.VERTEX_PREFIX) || this.name.startsWith(Topology.EDGE_PREFIX));
        //Need to set the AbstractLabel's partitionType and partitionExpression
        //Root PartitionTree are partitioned AbstractLabels.
        if (this.name.startsWith(Topology.VERTEX_PREFIX)) {
            TopologyManager.updateVertexLabelPartitionTypeAndExpression(
                    sqlgGraph,
                    this.schema,
                    this.name.substring(Topology.VERTEX_PREFIX.length()),
                    this.partitionType,
                    this.getPartitionExpression());
        } else {
            TopologyManager.updateEdgeLabelPartitionTypeAndExpression(
                    sqlgGraph,
                    this.schema,
                    this.name.substring(Topology.EDGE_PREFIX.length()),
                    this.partitionType,
                    this.getPartitionExpression());
        }
        //The root PartitionTree's immediate children are the first level Partition.
        //i.e. the Partition's parent is an AbstractLabel.
        for (PartitionTree child : this.children) {
            if (this.name.startsWith(Topology.VERTEX_PREFIX)) {
                if (this.partitionType.isRange()) {
                    Preconditions.checkState(child.fromExpression() != null);
                    Preconditions.checkState(child.toExpression() != null);
                    TopologyManager.addVertexLabelPartition(
                            sqlgGraph,
                            this.schema,
                            this.name.substring(Topology.VERTEX_PREFIX.length()),
                            child.name,
                            child.fromExpression(),
                            child.toExpression(),
                            child.partitionType,
                            child.getPartitionExpression());
                } else if (this.partitionType.isHash()) {
                    Preconditions.checkState(child.modulusExpression() != null);
                    Preconditions.checkState(child.remainderExpression() != null);
                    TopologyManager.addVertexLabelPartition(
                            sqlgGraph,
                            this.schema,
                            this.name.substring(Topology.VERTEX_PREFIX.length()),
                            child.name,
                            child.modulusExpression(),
                            child.remainderExpression(),
                            child.partitionType,
                            child.getPartitionExpression());
                } else {
                    Preconditions.checkState(child.inExpression() != null);
                    TopologyManager.addVertexLabelPartition(
                            sqlgGraph,
                            this.schema,
                            this.name.substring(Topology.VERTEX_PREFIX.length()),
                            child.name,
                            child.inExpression(),
                            child.partitionType,
                            child.getPartitionExpression());
                }
                child.walk(
                        sqlgGraph,
                        true,
                        schema,
                        this.name.substring(Topology.VERTEX_PREFIX.length())

                );
            } else {
                if (this.partitionType.isRange()) {
                    Preconditions.checkState(child.fromExpression() != null);
                    Preconditions.checkState(child.toExpression() != null);
                    TopologyManager.addEdgeLabelPartition(
                            sqlgGraph,
                            this.schema,
                            this.name.substring(Topology.EDGE_PREFIX.length()),
                            child.name,
                            child.fromExpression(),
                            child.toExpression(),
                            child.partitionType,
                            child.getPartitionExpression());
                } else if (this.partitionType.isHash()) {
                    Preconditions.checkState(child.modulusExpression() != null);
                    Preconditions.checkState(child.remainderExpression() != null);
                    TopologyManager.addEdgeLabelPartition(
                            sqlgGraph,
                            this.schema,
                            this.name.substring(Topology.EDGE_PREFIX.length()),
                            child.name,
                            child.modulusExpression(),
                            child.remainderExpression(),
                            child.partitionType,
                            child.getPartitionExpression());
                } else {
                    Preconditions.checkState(child.inExpression() != null);
                    TopologyManager.addEdgeLabelPartition(
                            sqlgGraph,
                            this.schema,
                            this.name.substring(Topology.EDGE_PREFIX.length()),
                            child.name,
                            child.inExpression(),
                            child.partitionType,
                            child.getPartitionExpression());
                }
                child.walk(
                        sqlgGraph,
                        false,
                        schema,
                        this.name.substring(Topology.EDGE_PREFIX.length())

                );
            }
        }
    }

    private void walk(SqlgGraph sqlgGraph, boolean isVertexLabel, String schema, String abstractLabel) {
        for (PartitionTree child : this.children) {
            TopologyManager.addSubPartition(
                    sqlgGraph,
                    child.parentPartitionTree.parentPartitionTree.parentPartitionTree != null,
                    isVertexLabel,
                    schema,
                    abstractLabel,
                    child.parentPartitionTree.name,
                    child.name,
                    child.partitionType,
                    child.getPartitionExpression(),
                    child.fromExpression(),
                    child.toExpression(),
                    child.inExpression(),
                    child.modulusExpression(),
                    child.remainderExpression()
            );
            child.walk(sqlgGraph, isVertexLabel, schema, abstractLabel);
        }
    }

    private String fromExpression() {
        if (this.fromToInModulusRemainder != null && this.parentPartitionTree.partitionType.isRange()) {
            String tmp = this.fromToInModulusRemainder.substring("FOR VALUES FROM (".length());
            String[] fromTo = tmp.split(" TO ");
            String from = fromTo[0].trim();
            from = StringUtils.removeStart(from, "(");
            from = StringUtils.removeEnd(from, ")");
            return from;
        } else {
            return null;
        }
    }

    private String toExpression() {
        if (this.fromToInModulusRemainder != null && this.parentPartitionTree.partitionType.isRange()) {
            String tmp = this.fromToInModulusRemainder.substring("FOR VALUES FROM (".length());
            String[] fromTo = tmp.split(" TO ");
            String to = fromTo[1].trim();
            to = StringUtils.removeStart(to, "(");
            to = StringUtils.removeEnd(to, ")");
            return to;
        } else {
            return null;
        }
    }

    private String inExpression() {
        if (this.fromToInModulusRemainder != null && this.parentPartitionTree.partitionType.isList()) {
            String tmp = this.fromToInModulusRemainder.substring("FOR VALUES IN (".length());
            String[] fromTo = tmp.split(" TO ");
            String in = fromTo[0];
            in = StringUtils.removeStart(in, "(");
            in = StringUtils.removeEnd(in, ")");
            return in;
        } else {
            return null;
        }
    }

    private Integer modulusExpression() {
        if (this.fromToInModulusRemainder != null && this.parentPartitionTree.partitionType.isHash()) {
            String tmp = this.fromToInModulusRemainder.substring("FOR VALUES WITH (".length());
            String[] modulusRemainder = tmp.split(",");
            String modulus = modulusRemainder[0];
            modulus = StringUtils.removeStart(modulus, "modulus ");
            return Integer.valueOf(modulus);
        } else {
            return null;
        }
    }

    private Integer remainderExpression() {
        if (this.fromToInModulusRemainder != null && this.parentPartitionTree.partitionType.isHash()) {
            String tmp = this.fromToInModulusRemainder.substring("FOR VALUES WITH (".length());
            String[] modulusRemainder = tmp.split(",");
            String remainder = modulusRemainder[1];
            remainder = StringUtils.removeStart(remainder, " remainder ");
            remainder = StringUtils.removeEnd(remainder, ")");
            return Integer.valueOf(remainder);
        } else {
            return null;
        }
    }

    private String getPartitionExpression() {
        return (this.partitionExpression1 != null ? this.partitionExpression1 : this.partitionExpression2);
    }

}
