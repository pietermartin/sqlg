package org.umlg.sqlg.process;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.javatuples.Pair;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.strategy.SqlgComparatorHolder;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.Comparator;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/09
 */
public class EmitComparator<E extends SqlgElement> implements Comparator<Emit<E>> {

    EmitComparator() {
    }

    @Override
    public int compare(Emit<E> emit1, Emit<E> emit2) {
        SqlgElement sqlgElement1 = null;
        SqlgElement sqlgElement2 = null;

        for (int i = emit1.getSqlgComparatorHolders().size() - 1; i >= 0; i--) {
            SqlgComparatorHolder sqlgComparatorHolder = emit1.getSqlgComparatorHolders().get(i);

            for (Pair<Traversal.Admin<?, ?>, Comparator<?>> comparatorPair : sqlgComparatorHolder.getComparators()) {
                Comparator<?> comparator = comparatorPair.getValue1();
                Traversal.Admin traversal = comparatorPair.getValue0();
                if (traversal.getSteps().size() == 1 && traversal.getSteps().get(0) instanceof SelectOneStep) {

                    //If the comparator's traversal has a SelectOneStep then the element is on the emit's path.
                    //xxxxx.select("a").order().by(select("a").by("name"), Order.decr)
                    SelectOneStep selectOneStep = (SelectOneStep) traversal.getSteps().get(0);
                    Preconditions.checkState(selectOneStep.getScopeKeys().size() == 1, "toOrderByClause expects the selectOneStep to have one scopeKey!");
                    Preconditions.checkState(selectOneStep.getLocalChildren().size() == 1, "toOrderByClause expects the selectOneStep to have one traversal!");
                    Preconditions.checkState(selectOneStep.getLocalChildren().get(0) instanceof ElementValueTraversal, "toOrderByClause expects the selectOneStep's traversal to be a ElementValueTraversal!");

                    String selectKey = (String) selectOneStep.getScopeKeys().iterator().next();

                    for (int j = emit1.getPath().labels().size() - 1; j >= 0; j--) {
                        Set<String> labels = emit1.getPath().labels().get(j);
                        if (labels.contains(selectKey)) {
                            sqlgElement1 = (SqlgElement) emit1.getPath().objects().get(j);
                            if (emit2.getPath().objects().size() > j) {
                                sqlgElement2 = (SqlgElement) emit2.getPath().objects().get(j);
                            }
                            break;
                        }
                    }
                    if (sqlgElement1 != null && sqlgElement2 != null) {
                        ElementValueTraversal elementValueTraversal = (ElementValueTraversal) selectOneStep.getLocalChildren().get(0);
                        int compare = comparator.compare(sqlgElement1.value(elementValueTraversal.getPropertyKey()), sqlgElement2.value(elementValueTraversal.getPropertyKey()));
                        if (compare != 0) {
                            return compare;
                        }
                    }
                } else if (traversal instanceof IdentityTraversal) {
                    return Order.shuffle.compare(null, null);
                } else {
                    if (sqlgComparatorHolder.getPrecedingSelectOneLabel() != null) {
                        int count = 0;
                        for (Set<String> labels : emit1.getPath().labels()) {
                            if (labels.contains(sqlgComparatorHolder.getPrecedingSelectOneLabel())) {
                                sqlgElement1 = (SqlgElement) emit1.getPath().objects().get(count);
                                if (emit2.getPath().objects().size() > count) {
                                    sqlgElement2 = (SqlgElement) emit2.getPath().objects().get(count);
                                }
                                break;
                            }
                        }
                    } else {
                        sqlgElement1 = (SqlgElement) emit1.getPath().objects().get(i);
                        if (emit2.getPath().objects().size() > i) {
                            sqlgElement2 = (SqlgElement) emit2.getPath().objects().get(i);
                        }
                    }

                    if (sqlgElement1 != null && sqlgElement2 != null) {
                        ElementValueTraversal elementValueTraversal = (ElementValueTraversal) traversal;
                        int compare = comparator.compare(sqlgElement1.value(elementValueTraversal.getPropertyKey()), sqlgElement2.value(elementValueTraversal.getPropertyKey()));
                        if (compare != 0) {
                            return compare;
                        }
                    } else if (sqlgElement1 != null)  {
                        return -1;
                    } else if (sqlgElement2 != null) {
                        return -1;
                    }
                }
            }
        }
        return 0;
    }

}
