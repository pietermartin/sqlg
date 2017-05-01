package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/05/01
 */
public class SqlgTraverser <T> extends B_LP_O_P_S_SE_SL_Traverser<T> {

    private long startElementIndex;

    public SqlgTraverser() {
    }

    public SqlgTraverser(T t, Step<T, ?> step, long initialBulk) {
        super(t, step, initialBulk);
    }

    public void setStartElementIndex(long startElementIndex) {
        this.startElementIndex = startElementIndex;
    }

    public long getStartElementIndex() {
        return startElementIndex;
    }

    //    @Override
//    public int compareTo(SqlgTraverser other) {
//        return (this.startElementIndex < other.startElementIndex) ? -1 : ((this.startElementIndex == other.startElementIndex) ? 0 : 1);
//    }
}
