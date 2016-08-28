package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.List;

interface SqlgStep<S extends SqlgElement, E extends SqlgElement> extends Step {

    List<ReplacedStep<S, E>> getReplacedSteps();

    void addReplacedStep(ReplacedStep<S, E> replacedStep);

    void parseForStrategy();

    boolean isForMultipleQueries();
}
