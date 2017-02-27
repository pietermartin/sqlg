package org.umlg.sqlg.process;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.javatuples.Pair;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/02/24
 */
public class EmitComparator<S extends SqlgElement, E extends SqlgElement, EE extends SqlgElement> implements Comparator<Emit> {

    private List<ReplacedStep<S, E>> replacedSteps;

    public EmitComparator(List<ReplacedStep<S, E>> replacedSteps) {
        this.replacedSteps = replacedSteps;
    }

    /**
     * We start comparing from the leaf nodes. i.e. from the outside in.
     */
    @Override
    public int compare(Emit emit1, Emit emit2) {
        for (int i = this.replacedSteps.size() - 1; i >= 0; i--) {
            ReplacedStep<S, E> replacedStep = this.replacedSteps.get(i);
            if (!replacedStep.getComparators().isEmpty()) {
//                if (sqlgElement1 != null && sqlgElement2 != null) {
                    for (Pair<Traversal.Admin, Comparator> object : replacedStep.getComparators()) {
                        if (object.getValue1() instanceof ElementValueComparator) {
                            SqlgElement sqlgElement1 = null;
                            SqlgElement sqlgElement2 = null;
                            if (emit1.getPath().size() > i) {
                                sqlgElement1 = (SqlgElement) emit1.getPath().objects().get(i);
                            }
                            if (emit2.getPath().size() > i) {
                                sqlgElement2 = (SqlgElement) emit2.getPath().objects().get(i);
                            }
                            if (sqlgElement1 != null && sqlgElement2 != null) {
                                ElementValueComparator elementValueComparator = (ElementValueComparator) object.getValue1();
                                int compare = elementValueComparator.compare(sqlgElement1.value(elementValueComparator.getPropertyKey()), sqlgElement2.value(elementValueComparator.getPropertyKey()));
                                if (compare != 0) {
                                    return compare;
                                }
                            }
                        } else if (object.getValue0() instanceof ElementValueTraversal && object.getValue1() instanceof Order) {
                            SqlgElement sqlgElement1 = null;
                            SqlgElement sqlgElement2 = null;
                            if (emit1.getPath().size() > i) {
                                sqlgElement1 = (SqlgElement) emit1.getPath().objects().get(i);
                            }
                            if (emit2.getPath().size() > i) {
                                sqlgElement2 = (SqlgElement) emit2.getPath().objects().get(i);
                            }
                            if (sqlgElement1 != null && sqlgElement2 != null) {
                                ElementValueTraversal elementValueTraversal = (ElementValueTraversal) object.getValue0();
                                Comparator<SqlgElement> comparator = object.getValue1();
                                int compare = comparator.compare(sqlgElement1.value(elementValueTraversal.getPropertyKey()), sqlgElement2.value(elementValueTraversal.getPropertyKey()));
                                if (compare != 0) {
                                    return compare;
                                }
                            }
                        } else {
                            Preconditions.checkState(object.getValue0().getSteps().size() == 1, "toOrderByClause expects a TraversalComparator to have exactly one step!");
                            Preconditions.checkState(object.getValue0().getSteps().get(0) instanceof SelectOneStep, "toOrderByClause expects a TraversalComparator to have exactly one SelectOneStep!");

                            SelectOneStep selectOneStep = (SelectOneStep) object.getValue0().getSteps().get(0);
                            Preconditions.checkState(selectOneStep.getScopeKeys().size() == 1, "toOrderByClause expects the selectOneStep to have one scopeKey!");
                            Preconditions.checkState(selectOneStep.getLocalChildren().size() == 1, "toOrderByClause expects the selectOneStep to have one traversal!");
                            Preconditions.checkState(selectOneStep.getLocalChildren().get(0) instanceof ElementValueTraversal, "toOrderByClause expects the selectOneStep's traversal to be a ElementValueTraversal!");

                            SqlgElement sqlgElement1 = null;
                            SqlgElement sqlgElement2 = null;
                            String selectKey = (String) selectOneStep.getScopeKeys().iterator().next();
                            //Go up the path to find the element with the selectKey label
                            int count = 0;
                            boolean foundLabel = false;
                            for (Set<String> labels : emit1.getPath().labels()) {
                                if (labels.contains(selectKey)) {
                                    foundLabel = true;
                                    break;
                                }
                                count++;
                            }
                            if (foundLabel) {
                                sqlgElement1 = (SqlgElement) emit1.getPath().objects().get(count);
                            }
                            count = 0;
                            foundLabel = false;
                            for (Set<String> labels : emit2.getPath().labels()) {
                                if (labels.contains(selectKey)) {
                                    foundLabel = true;
                                    break;
                                }
                                count++;
                            }
                            if (foundLabel) {
                                sqlgElement2 = (SqlgElement) emit2.getPath().objects().get(count);
                            }
                            if (sqlgElement1 != null && sqlgElement2 != null) {
                                ElementValueTraversal elementValueTraversal = (ElementValueTraversal) selectOneStep.getLocalChildren().get(0);
                                Comparator<SqlgElement> comparator = object.getValue1();

                                Property sqlgElement1Property = sqlgElement1.property(elementValueTraversal.getPropertyKey());
                                Property sqlgElement2Property = sqlgElement2.property(elementValueTraversal.getPropertyKey());
                                if (sqlgElement1Property.isPresent() && !sqlgElement2Property.isPresent()) {
                                    return 1;
                                } else if (!sqlgElement1Property.isPresent() && sqlgElement2Property.isPresent()) {
                                    return -1;
                                } else if (!sqlgElement1Property.isPresent() && !sqlgElement2Property.isPresent()) {
                                    return 0;
                                } else {
                                    int compare = comparator.compare(sqlgElement1.value(elementValueTraversal.getPropertyKey()), sqlgElement2.value(elementValueTraversal.getPropertyKey()));
                                    if (compare != 0) {
                                        return compare;
                                    }
                                }
                            }

                        }
                    }
//                }
            }
        }
        return 0;
    }

}
