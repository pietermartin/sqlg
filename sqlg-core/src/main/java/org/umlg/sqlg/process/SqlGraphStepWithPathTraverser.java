package org.umlg.sqlg.process;

import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_P_S_SE_SL_Traverser;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by pieter on 2015/07/20.
 */
public class SqlGraphStepWithPathTraverser<T> extends B_O_P_S_SE_SL_Traverser<T> implements SqlgLabelledPathTraverser {

    private Multimap<String, Object> labeledObjects;

    public SqlGraphStepWithPathTraverser(final T t, Multimap<String, Object> labeledObjects, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
        this.labeledObjects = labeledObjects;
        if (!this.labeledObjects.isEmpty()) {
            customSplit();
        }
    }

    /**
     * This odd logic is to ensure the path represents the path from left to right.
     * Calling this.path.extends(...) reverses the path. The test still pass but it seems wrong.
     */
    private void customSplit() {
        Path localPath = ImmutablePath.make();
        for (String label : labeledObjects.keySet()) {
            Collection<Object> labeledElements = labeledObjects.get(label);
            for (Object labeledElement : labeledElements) {
                localPath = localPath.extend(labeledElement, Collections.singleton(label));
            }
        }
        this.path = localPath;
    }

    @Override
    public void setPath(Path path) {
        this.path = path;
    }
}
