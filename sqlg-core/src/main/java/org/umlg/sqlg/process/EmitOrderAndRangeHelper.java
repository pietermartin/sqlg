package org.umlg.sqlg.process;

import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.strategy.BaseStrategy;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/09
 */
public class EmitOrderAndRangeHelper<E extends SqlgElement> {

    private List<ReplacedStep<?, ?>> replacedSteps;
//    private List<Emit<E>> eagerLoadedResults;
    private List<Emit<E>> eagerLoadedResults;

//    public EmitOrderAndRangeHelper(List<Emit<E>> eagerLoadedResults, List<ReplacedStep<?, ?>> replacedSteps) {
//        this.eagerLoadedResults = eagerLoadedResults;
//        this.replacedSteps = replacedSteps;
//    }

    public EmitOrderAndRangeHelper(List<Emit<E>> eagerLoadedResults, List<ReplacedStep<?, ?>> replacedSteps) {
        this.eagerLoadedResults = eagerLoadedResults;
        this.replacedSteps = replacedSteps;
    }

    public void sortAndApplyRange() {
        this.eagerLoadedResults.sort(new EmitComparator<>());
//        ReplacedStep<?, ?> lastReplacedStep = this.replacedSteps.get(this.replacedSteps.size() - 1);
//        if (lastReplacedStep.hasRange()) {
//            applyRange(lastReplacedStep);
//        }
    }

    private void applyRange(ReplacedStep<?, ?> replacedStep) {
        Iterator<Emit<E>> iterator = this.eagerLoadedResults.iterator();
        while (iterator.hasNext()) {
            Emit<E> emit = iterator.next();
            if (!emit.getLabels().isEmpty()) {
                boolean foundLabel = false;
                for (Set<String> labels : emit.getPath().labels()) {
                    for (String label : labels) {
                        if (replacedStep.getLabels().contains(replacedStep.getDepth() + BaseStrategy.PATH_LABEL_SUFFIX + label) ||
                                replacedStep.getLabels().contains(replacedStep.getDepth() + BaseStrategy.EMIT_LABEL_SUFFIX + label) ||
                                replacedStep.getLabels().contains(replacedStep.getDepth() + BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL + label)) {
                            foundLabel = true;
                            break;
                        }
                    }
                    if (foundLabel) {
                        break;
                    }
                }
                if (foundLabel && !replacedStep.containsRange()) {
                    iterator.remove();
                }
            } else {
                if (!replacedStep.containsRange()) {
                    iterator.remove();
                }
            }
        }
    }
}
