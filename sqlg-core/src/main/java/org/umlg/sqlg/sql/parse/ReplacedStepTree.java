package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.javatuples.Pair;
import org.umlg.sqlg.strategy.BaseStrategy;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.*;

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

    void walkReplacedSteps(Set<SchemaTableTree> schemaTableTrees) {
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
            if (!replacedStep.isFake() && !(replacedStep.getStep() instanceof OrderGlobalStep) && !(replacedStep.getStep() instanceof RangeGlobalStep)) {
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

    private List<TreeNode> leafNodes() {
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

    public boolean hasRange() {
        List<ReplacedStep<?,?>> replacedSteps = linearPathToLeafNode();
        ReplacedStep<?, ?> replacedStep = replacedSteps.get(replacedSteps.size() - 1);
        return replacedStep.hasRange();
    }

    public boolean hasOrderBy() {
        for (ReplacedStep<?, ?> replacedStep : linearPathToLeafNode()) {
            if (replacedStep.getSqlgComparatorHolder().hasComparators()) {
                return true;
            }
        }
        return false;
    }

    /**
     * This happens when a SqlgVertexStep has a SelectOne step where the label is for an element on the path
     * that is before the current optimized steps.
     * @return
     */
    public boolean orderByHasSelectOneStepAndForLabelNotInTree() {
        Set<String> labels = new HashSet<>();
        for (ReplacedStep<?, ?> replacedStep : linearPathToLeafNode()) {
            for (String label : labels) {
                labels.add(SqlgUtil.originalLabel(label));
            }
            for (Pair<Traversal.Admin<?, ?>, Comparator<?>> objects : replacedStep.getSqlgComparatorHolder().getComparators()) {
                Traversal.Admin<?, ?> traversal = objects.getValue0();
                if (traversal.getSteps().size() == 1 && traversal.getSteps().get(0) instanceof SelectOneStep) {
                    //xxxxx.select("a").order().by(select("a").by("name"), Order.decr)
                    SelectOneStep selectOneStep = (SelectOneStep) traversal.getSteps().get(0);
                    Preconditions.checkState(selectOneStep.getScopeKeys().size() == 1, "toOrderByClause expects the selectOneStep to have one scopeKey!");
                    Preconditions.checkState(selectOneStep.getLocalChildren().size() == 1, "toOrderByClause expects the selectOneStep to have one traversal!");
                    Preconditions.checkState(
                            selectOneStep.getLocalChildren().get(0) instanceof ElementValueTraversal ||
                                    selectOneStep.getLocalChildren().get(0) instanceof TokenTraversal
                            ,
                            "toOrderByClause expects the selectOneStep's traversal to be a ElementValueTraversal or a TokenTraversal!");
                    String selectKey = (String) selectOneStep.getScopeKeys().iterator().next();
                    if (!labels.contains(selectKey)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean orderByIsOrder() {
        for (ReplacedStep<?, ?> replacedStep : linearPathToLeafNode()) {
            for (Pair<Traversal.Admin<?, ?>, Comparator<?>> objects : replacedStep.getSqlgComparatorHolder().getComparators()) {
                if (!(objects.getValue1() instanceof Order && objects.getValue1() != Order.shuffle)) {
                    return false;
                }
            }
        }
        return true;
    }

    public void applyComparatorsOnDb() {
        for (ReplacedStep<?, ?> replacedStep : linearPathToLeafNode()) {
            for (Pair<Traversal.Admin<?, ?>, Comparator<?>> comparatorPair : replacedStep.getSqlgComparatorHolder().getComparators()) {
                replacedStep.getDbComparators().add(comparatorPair);
            }
        }
    }

    public void doNotApplyRangeOnDb() {
        List<ReplacedStep<?,?>> replacedSteps = linearPathToLeafNode();
        ReplacedStep<?, ?> replacedStep = replacedSteps.get(replacedSteps.size() - 1);
        Preconditions.checkState(replacedStep.hasRange());
        replacedStep.getSqlgRangeHolder().doNotApplyOnDb();
    }

    public void doNotApplyInStep() {
        List<ReplacedStep<?,?>> replacedSteps = linearPathToLeafNode();
        ReplacedStep<?, ?> replacedStep = replacedSteps.get(replacedSteps.size() - 1);
        Preconditions.checkState(replacedStep.hasRange());
        replacedStep.getSqlgRangeHolder().doNotApplyInStep();
    }

    public void reset() {
        List<ReplacedStep<?,?>> replacedSteps = linearPathToLeafNode();
        ReplacedStep<?, ?> replacedStep = replacedSteps.get(replacedSteps.size() - 1);
        //TODO remove this null check
        if (replacedStep.getSqlgRangeHolder() != null) {
            replacedStep.getSqlgRangeHolder().reset();
        }
    }

    public class TreeNode {
        private final ReplacedStep replacedStep;
        private TreeNode parent;
        private final List<TreeNode> children = new ArrayList<>();
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
            return this.replacedStep.toString();
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
