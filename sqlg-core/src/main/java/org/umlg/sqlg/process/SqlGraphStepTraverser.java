package org.umlg.sqlg.process;

import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_P_S_SE_SL_Traverser;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by pieter on 2015/07/20.
 */
public class SqlGraphStepTraverser<T, E> extends B_O_P_S_SE_SL_Traverser<T> {

    private Multimap<String, Object> labeledObjects;
    private Step sqlgGraphStep;

    public SqlGraphStepTraverser(final T t, Multimap<String, Object> labeledObjects, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
        this.sqlgGraphStep = step;
        this.labeledObjects = labeledObjects;
        customSplit();
    }

    private void customSplit() {
        //split before setting the path.
        //This is because the labels must be set on a unique path for every iteration.
        Traverser.Admin<E> split = this.split(t, this.sqlgGraphStep);
        for (String label : labeledObjects.keySet()) {
            //If there are labels then it must be a B_O_P_S_SE_SL_Traverser
            SqlGraphStepTraverser b_o_p_s_se_sl_traverser = (SqlGraphStepTraverser)split;
            Collection<Object> labeledElements = labeledObjects.get(label);
            for (Object labeledElement : labeledElements) {
                b_o_p_s_se_sl_traverser.setPath(split.path().extend(labeledElement, Collections.singleton(label)));
            }
        }
    }

}
