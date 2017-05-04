package org.umlg.sqlg.util;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.SqlgChooseStepBarrier;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/29
 */
public class SqlgTraversalUtil {

    public static Pair<Traversal.Admin<?, ?>, Traversal.Admin<?, ?>> trueFalseTraversals(SqlgChooseStepBarrier<?, ?, ?> sqlgChooseStepBarrier) {
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

    public static boolean isOptionalChooseStep(ChooseStep<?, ?, ?> chooseStep) {
        List<? extends Traversal.Admin<?, ?>> traversalAdmins = chooseStep.getGlobalChildren();
        if (traversalAdmins.size() != 2) {
            return false;
        }
        Traversal.Admin<?, ?> predicate = chooseStep.getLocalChildren().get(0);
        List<Step> predicateSteps = new ArrayList<>(predicate.getSteps());

        Traversal.Admin<?, ?> globalChildOne = chooseStep.getGlobalChildren().get(0);
        Traversal.Admin<?, ?> globalChildTwo = chooseStep.getGlobalChildren().get(1);

        List<Step> globalChildOneSteps = new ArrayList<>(globalChildOne.getSteps());
        globalChildOneSteps.remove(globalChildOneSteps.size() - 1);
        List<Step> globalChildTwoSteps = new ArrayList<>(globalChildTwo.getSteps());
        globalChildTwoSteps.remove(globalChildTwoSteps.size() - 1);

        boolean hasIdentity = globalChildOne.getSteps().stream().anyMatch(s -> s instanceof IdentityStep);
        if (!hasIdentity) {
            hasIdentity = globalChildTwo.getSteps().stream().anyMatch(s -> s instanceof IdentityStep);
            if (hasIdentity) {
                //Identity found check predicate and true are the same
                if (!predicateSteps.equals(globalChildOneSteps)) {
                    return false;
                }
            } else {
                //Identity not found
                return false;
            }
        } else {
            //Identity found check predicate and true are the same
            if (!predicateSteps.equals(globalChildTwoSteps)) {
                return false;
            }
        }
        return true;
    }
}
