package org.umlg.sqlg.process;

import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.strategy.BaseStrategy;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/09
 */
public class EmitOrderAndRangeHelper<E extends SqlgElement> {

    private List<ReplacedStep<?, ?>> replacedSteps;
    private List<Emit<E>> eagerLoadedResults;

    public EmitOrderAndRangeHelper(List<Emit<E>> eagerLoadedResults, List<ReplacedStep<?, ?>> replacedSteps) {
        this.eagerLoadedResults = eagerLoadedResults;
        this.replacedSteps = replacedSteps;
    }

    public void sortAndApplyRange() {
        //Check for presort
        //If there is a range that is not the leaf step the first apply the range.
        //If the range follows an order then first order up to that step.


        //This first loop is to check if there are any steps before the last step that has a range.
        //If so first apply the range.
        int count = 1;
        ReplacedStep<?,?> lastReplacedStep = null;
        for (ReplacedStep<?, ?> replacedStep : this.replacedSteps) {
            if (replacedStep.hasRange()) {
                //only for replacedSteps before the last one check
                if (count < this.replacedSteps.size()) {
                    if (replacedStep.getSqlgComparatorHolder().hasComparators()) {
                        //First order the whole result set then limit
                        this.eagerLoadedResults.sort(new ReplacedStepEmitComparator<>());
                    }
                    applyRange(replacedStep);
                }
            }
            lastReplacedStep = replacedStep;
            count++;
        }
        //Now sort the whole lot.
        this.eagerLoadedResults.sort(new ReplacedStepEmitComparator<>());
        if (lastReplacedStep.hasRange()) {
            applyRange(lastReplacedStep);
        }
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
                                replacedStep.getLabels().contains(replacedStep.getDepth() + BaseStrategy.EMIT_LABEL_SUFFIX + label)) {
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
