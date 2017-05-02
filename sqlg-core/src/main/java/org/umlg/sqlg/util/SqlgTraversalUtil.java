package org.umlg.sqlg.util;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.SqlgChooseStepBarrier;

import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/29
 */
public class SqlgTraversalUtil {

    public static Pair<Traversal.Admin<?, ?>, Traversal.Admin<?, ?>> trueFalseTraversals(SqlgChooseStepBarrier<?,?,?> sqlgChooseStepBarrier) {
        return Pair.of(sqlgChooseStepBarrier.getTraversalOptions().get(Boolean.TRUE).get(0), sqlgChooseStepBarrier.getTraversalOptions().get(Boolean.FALSE).get(0));
    }

    public static boolean hasOneBulkRequirement(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = TraversalHelper.getStepsOfAssignableClassRecursively(Step.class, traversal);
        for (Step step : steps) {
            if (step.getRequirements().contains(TraverserRequirement.ONE_BULK)) {
                return true;
            }
        }
        return false;
    }
}
