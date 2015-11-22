package org.umlg.sqlg.process;

import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_P_S_SE_SL_Traverser;
import org.umlg.sqlg.strategy.BaseSqlgStrategy;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.structure.Dummy;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.*;

/**
 * Created by pieter on 2015/07/20.
 */
public class SqlGraphStepWithPathTraverser<T, E extends SqlgElement> extends B_O_P_S_SE_SL_Traverser<T> implements SqlgLabelledPathTraverser {

    private List<Emit> toEmit = new ArrayList<>();

    public SqlGraphStepWithPathTraverser(final T t, Multimap<String, Emit<E>> labeledObjects, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
        if (labeledObjects != null && !labeledObjects.isEmpty()) {
            Path localPath = ImmutablePath.make();
            customSplit(t, localPath, labeledObjects);
        }
    }

    public SqlGraphStepWithPathTraverser(final T t, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
    }

    public List<Emit> getToEmit() {
        return this.toEmit;
    }

    /**
     * This odd logic is to ensure the path represents the path from left to right.
     * Calling this.path.extends(...) reverses the path. The test still pass but it seems wrong.
     */
    public void customSplit(final T t, Path currentPath, Multimap<String, Emit<E>> labeledObjects) {
        boolean addT = true;
        List<String> sortedKeys = new ArrayList<>(labeledObjects.keySet());
        Collections.sort(sortedKeys);
        //This is to prevent duplicates in the path. Each labeled object will be present in the sql result set.
        //If the same object has multiple labels it will be present many times in the sql result set.
        //The  allLabeledElementsAsSet undoes this duplication by ensuring that there is only one path for the object with multiple labels.
        Map<String, Set<Object>> allLabeledElementMap = new HashMap<>();
        for (String label : sortedKeys) {
            Collection<Emit<E>> labeledElements = labeledObjects.get(label);
            String realLabel;
            String pathLabel;
            int degree;
            if (label.contains(BaseSqlgStrategy.PATH_LABEL_SUFFIX)) {
                realLabel = label.substring(label.indexOf(BaseSqlgStrategy.PATH_LABEL_SUFFIX) + BaseSqlgStrategy.PATH_LABEL_SUFFIX.length());
                pathLabel = label.substring(0, label.indexOf(BaseSqlgStrategy.PATH_LABEL_SUFFIX) + BaseSqlgStrategy.PATH_LABEL_SUFFIX.length());
                degree = Integer.valueOf(pathLabel.substring(0, pathLabel.indexOf(BaseSqlgStrategy.PATH_LABEL_SUFFIX)));
            } else if (label.contains(BaseSqlgStrategy.EMIT_LABEL_SUFFIX)) {
                realLabel = label.substring(label.indexOf(BaseSqlgStrategy.EMIT_LABEL_SUFFIX) + BaseSqlgStrategy.EMIT_LABEL_SUFFIX.length());
                pathLabel = label.substring(0, label.indexOf(BaseSqlgStrategy.EMIT_LABEL_SUFFIX) + BaseSqlgStrategy.EMIT_LABEL_SUFFIX.length());
                degree = Integer.valueOf(pathLabel.substring(0, pathLabel.indexOf(BaseSqlgStrategy.EMIT_LABEL_SUFFIX)));
            } else {
                throw new IllegalStateException();
            }
            for (Emit<E> emit : labeledElements) {
                if (addT && emit.getElementPlusEdgeId().getLeft().equals(t)) {
                    addT = false;
                }
                Set<Object> allLabeledElementsAsSet = allLabeledElementMap.get(pathLabel);
                if (allLabeledElementsAsSet == null) {
                    allLabeledElementsAsSet = new HashSet<>();
                    allLabeledElementMap.put(pathLabel, allLabeledElementsAsSet);
                }
                if (!allLabeledElementsAsSet.contains(emit.getElementPlusEdgeId().getLeft())) {
                    currentPath = currentPath.extend(emit.getElementPlusEdgeId().getLeft(), Collections.singleton(realLabel));
                    allLabeledElementsAsSet.add(emit.getElementPlusEdgeId().getLeft());
                    if (pathLabel.endsWith(BaseSqlgStrategy.EMIT_LABEL_SUFFIX)) {
                        emit.setPath(currentPath.clone());
                        emit.setDegree(degree);
                        this.toEmit.add(emit);
                    }
                } else {
                    currentPath.addLabel(realLabel);
                }
            }
        }
        if (addT && !(t instanceof Dummy)) {
            //tp relies on all elements traversed being on the path.
            //if the element is not labelled put it on the path
            currentPath = currentPath.clone().extend(t);
        }
        this.path = currentPath;
    }

    @Override
    public void setPath(Path path) {
        this.path = path;
    }

    @Override
    public Path getPath() {
        return this.path;
    }
}
