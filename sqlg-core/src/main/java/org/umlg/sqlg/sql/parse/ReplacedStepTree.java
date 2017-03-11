package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.umlg.sqlg.strategy.BaseStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/03
 */
public class ReplacedStepTree {

    private TreeNode currentTreeNodeNode;
    private int depth;

    public ReplacedStepTree(ReplacedStep replacedStep) {
        this.currentTreeNodeNode = new TreeNode(replacedStep);
        this.currentTreeNodeNode.depth = 0;
        this.depth = 0;
    }

    public void addReplacedStep(ReplacedStep replacedStep) {
        this.currentTreeNodeNode = this.currentTreeNodeNode.addReplacedStep(replacedStep);
    }

    public TreeNode getCurrentTreeNodeNode() {
        return currentTreeNodeNode;
    }

    public TreeNode root() {
        return this.currentTreeNodeNode.root();
    }

    public int getDepth() {
        return depth;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        internalToString(1, this.currentTreeNodeNode.root(), sb);
        return sb.toString();
    }

    private void internalToString(int depth, TreeNode treeNode, StringBuilder sb) {
        sb.append(treeNode.toString());
        sb.append("\n");
        for (TreeNode child : treeNode.children) {
            for (int i = 0; i < depth; i++) {
                sb.append("\t");
            }
            internalToString(depth + 1, child, sb);
        }
    }

    public void walkReplacedSteps(Set<SchemaTableTree> schemaTableTrees) {
        //The tree only has one linear path from root to the deepest leaf node.
        //This represents the regular path where each ReplacedStep goes one step deeper down the graph.
        //First build the SchemaTableTrees for this path.
        //The other nodes in this ReplacedStepTree are nodes that need to join onto the left join nodes coming from optional steps.
        List<ReplacedStep<?, ?>> replacedSteps = linearPathToLeafNode();

        for (ReplacedStep<?, ?> replacedStep : replacedSteps) {
            //skip the graph step
            if (replacedStep.getStep() instanceof GraphStep) {
                continue;
            }
            if (!(replacedStep.getStep() instanceof OrderGlobalStep) && !(replacedStep.getStep() instanceof RangeGlobalStep)) {
                //This schemaTableTree represents the tree nodes as build up to this depth. Each replacedStep goes a level further
                schemaTableTrees = replacedStep.calculatePathForStep(schemaTableTrees);
            }
        }

    }

    private List<ReplacedStep<?, ?>> linearPathToLeafNode() {
        List<ReplacedStep<?, ?>> result = new ArrayList<>();
        this.root().internalLinearPathToLeafNode(result);
        return result;
    }

    public List<TreeNode> leafNodes() {
        return root().leafNodes();
    }

    public void maybeAddLabelToLeafNodes() {
        List<TreeNode> leafNodes = this.leafNodes();
        for (TreeNode leafNode : leafNodes) {
            ReplacedStep<?,?> replacedStep = leafNode.getReplacedStep();
            if (!replacedStep.isEmit() && replacedStep.getLabels().isEmpty()) {
                replacedStep.addLabel((leafNode.depth) + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
            }
        }
    }

    public class TreeNode {
        private ReplacedStep replacedStep;
        private TreeNode parent;
        private List<TreeNode> children = new ArrayList<>();
        private int depth = 0;

        TreeNode(ReplacedStep replacedStep) {
            this.replacedStep = replacedStep;
        }

        public ReplacedStepTree getReplacedStepTree() {
            return ReplacedStepTree.this;
        }

        public ReplacedStep getReplacedStep() {
            return replacedStep;
        }

        public TreeNode addReplacedStep(ReplacedStep replacedStep) {
            TreeNode treeNode = new TreeNode(replacedStep);
            treeNode.parent = this;
            this.children.add(treeNode);
            treeNode.depth = this.depth + 1;
            if (ReplacedStepTree.this.depth < treeNode.depth) {
                ReplacedStepTree.this.depth = treeNode.depth;
            }
            return treeNode;
        }

        TreeNode root() {
            if (this.parent != null) {
                TreeNode root = this.parent.root();
                if (root != null) {
                    return root;
                }
            }
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(this.replacedStep.toString());
            return sb.toString();
        }

        void internalLinearPathToLeafNode(List<ReplacedStep<?, ?>> result) {
            result.add(this.getReplacedStep());
            Preconditions.checkState(this.children.stream().filter(t -> !t.getReplacedStep().isJoinToLeftJoin()).count() <= 1);
            for (TreeNode child : this.children) {
                if (!child.getReplacedStep().isJoinToLeftJoin()) {
                    child.internalLinearPathToLeafNode(result);
                }
            }
        }

        List<TreeNode> leafNodes() {
            List<TreeNode> leafNodes = new ArrayList<>();
            internalLeafNodes(leafNodes);
            return leafNodes;
        }

        private void internalLeafNodes(List<TreeNode> leafNodes) {
            if (this.children.isEmpty()) {
                leafNodes.add(this);
            }
            for (TreeNode child : children) {
                child.internalLeafNodes(leafNodes);
            }
        }
    }


}
