package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.List;

public interface SqlgStep<S extends SqlgElement, E extends SqlgElement> extends Step {

    List<ReplacedStep<S, E>> getReplacedSteps();

    void addReplacedStep(ReplacedStep<S, E> replacedStep);

    void parseForStrategy();

    boolean isForMultipleQueries();

    default boolean rootEmitTreeContains(List<EmitTree<E>> rootEmitTrees, Emit emit) {
        for (EmitTree<E> rootEmitTree : rootEmitTrees) {
            if (rootEmitTree.getEmit().getPath().get(0).equals(emit.getPath().get(0)) &&
                    rootEmitTree.getEmit().getElementPlusEdgeId().equals(emit.getElementPlusEdgeId())) {

                return true;
            }
        }
        return false;
    }
}
