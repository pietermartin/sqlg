package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;

import java.util.List;

interface SqlgStep extends Step {

    List<ReplacedStep<?, ?>> getReplacedSteps();

    ReplacedStepTree.TreeNode addReplacedStep(ReplacedStep<?, ?> replacedStep);

    void parseForStrategy();

    boolean isForMultipleQueries();

    void setEagerLoad(boolean eager);

    boolean isEargerLoad();
}
