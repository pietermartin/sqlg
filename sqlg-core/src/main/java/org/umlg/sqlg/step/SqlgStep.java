package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;

import java.util.List;

public interface SqlgStep extends Step {

    List<ReplacedStep<?, ?>> getReplacedSteps();

    ReplacedStepTree.TreeNode addReplacedStep(ReplacedStep<?, ?> replacedStep);

    boolean isForMultipleQueries();

    void setEagerLoad(boolean eager);

    boolean isEagerLoad();
}
