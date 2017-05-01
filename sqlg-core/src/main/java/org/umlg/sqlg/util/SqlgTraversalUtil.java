package org.umlg.sqlg.util;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.umlg.sqlg.step.SqlgChooseStepBarrier;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/29
 */
public class SqlgTraversalUtil {

    public static Pair<Traversal.Admin<?, ?>, Traversal.Admin<?, ?>> trueFalseTraversals(SqlgChooseStepBarrier<?,?,?> sqlgChooseStepBarrier) {
        return Pair.of(sqlgChooseStepBarrier.getTraversalOptions().get(Boolean.TRUE).get(0), sqlgChooseStepBarrier.getTraversalOptions().get(Boolean.FALSE).get(0));
    }
}
