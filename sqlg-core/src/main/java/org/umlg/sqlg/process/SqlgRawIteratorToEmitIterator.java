package org.umlg.sqlg.process;

import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.umlg.sqlg.strategy.BaseSqlgStrategy;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.strategy.EmitTree;
import org.umlg.sqlg.strategy.OptionalLeaveNodes;
import org.umlg.sqlg.structure.Dummy;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.*;
import java.util.function.Supplier;

/**
 * Date: 2016/04/04
 * Time: 8:54 PM
 */
public class SqlgRawIteratorToEmitIterator<E extends SqlgElement> implements Iterator<Emit<E>>, Supplier<Iterator<Emit<E>>> {

    private List<EmitTree<E>> rootEmitTrees = new ArrayList<>();
    private EmitTree<E> currentEmitTree;
    private OptionalLeaveNodes<E> currentOptionalLeaveNodes;

    private Supplier<Iterator<Pair<E, Multimap<String, Emit<E>>>>> supplier;
    private Iterator<Pair<E, Multimap<String, Emit<E>>>> iterator;
    private boolean hasStarted;
    private Path currentPath;
    private List<Emit<E>> toEmit = new ArrayList<>();

    public SqlgRawIteratorToEmitIterator(Supplier<Iterator<Pair<E, Multimap<String, Emit<E>>>>> supplier) {
        this.supplier = supplier;
    }

    public SqlgRawIteratorToEmitIterator(Iterator<Pair<E, Multimap<String, Emit<E>>>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (!this.hasStarted) {
            if (this.iterator == null) {
                this.iterator = this.supplier.get();
            }
            this.hasStarted = true;
            return toEmitTree();
        } else {
            if (!this.toEmit.isEmpty()) {
                return true;
            } else {
                return toEmitTree();
            }
        }
    }

    @Override
    public Emit<E> next() {
        return this.toEmit.remove(0);
    }

    private boolean toEmitTree() {

        boolean result = false;
        while (!result) {
            List<Emit<E>> flattenedEmit = flattenRawIterator();
            if (!flattenedEmit.isEmpty()) {
                Iterator<Emit<E>> iter = flattenedEmit.iterator();
                while (iter.hasNext()) {
                    Emit<E> emit = iter.next();
                    iter.remove();

                    if (emit.isUseCurrentEmitTree()) {

                        if (this.currentEmitTree == null) {
                            //get it from the roots
                            for (EmitTree<E> rootEmitTree : rootEmitTrees) {
                                if (rootEmitTree.getEmit().getElementPlusEdgeId().equals(emit.getElementPlusEdgeId())) {
                                    this.currentEmitTree = rootEmitTree;
                                    break;
                                }
                            }
                        }

                        if ((this.rootEmitTrees.isEmpty() || (!this.rootEmitTrees.isEmpty() && this.currentEmitTree == null)) ||
                                (!rootEmitTreeContains(this.rootEmitTrees, emit) && !this.currentEmitTree.hasChild(emit.getElementPlusEdgeId()))) {

                            if (this.currentEmitTree == null) {
                                this.currentEmitTree = new EmitTree<>(emit);
                                this.rootEmitTrees.add(this.currentEmitTree);
                            } else {
                                this.currentEmitTree = this.currentEmitTree.addEmit(emit);
                            }
                            if (!(emit.getElementPlusEdgeId().getLeft() instanceof Dummy)) {
                                this.toEmit.add(emit);
                                result = true;
                            }

                        } else {
                            if (!this.currentEmitTree.getEmit().getElementPlusEdgeId().equals(emit.getElementPlusEdgeId())) {
                                this.currentEmitTree = this.currentEmitTree.getChild(emit.getElementPlusEdgeId());
                            }
                        }

                    } else if (emit.isUseOptionalTree()) {

//                        if (this.currentOptionalLeaveNodes == null) {
//                            this.currentOptionalLeaveNodes = new OptionalLeaveNodes<>(emit.getNumberOfSteps());
//                        }
//                        if (!this.currentOptionalLeaveNodes.containsPath(emit.getPath())) {
//                        this.currentOptionalLeaveNodes.addPath(emit.getPath());
                        this.toEmit.add(emit);
                        result = true;
//                        }

                    } else {

                        //currentEmitTree logic is only for repeat emittings
                        if (!(emit.getElementPlusEdgeId().getLeft() instanceof Dummy)) {
                            this.toEmit.add(emit);
                            result = true;
                        }

                    }
                }
            } else {
                //no more raw results so exit the loop. result will be false;
                break;
            }
            this.currentEmitTree = null;
        }
        return result;
    }

    private List<Emit<E>> flattenRawIterator() {
        List<Emit<E>> flattenedEmit = new ArrayList<>();
        if (this.iterator.hasNext()) {
            Emit<E> lastEmit = null;
            boolean addElement = true;
            this.currentPath = ImmutablePath.make();
            Pair<E, Multimap<String, Emit<E>>> raw = this.iterator.next();
            E element = raw.getLeft();
            Multimap<String, Emit<E>> labeledElements = raw.getRight();
            if (!labeledElements.isEmpty()) {
                List<String> sortedKeys = new ArrayList<>(labeledElements.keySet());
                Collections.sort(sortedKeys);
                //This is to prevent duplicates in the path. Each labeled object will be present in the sql result set.
                //If the same object has multiple labels it will be present many times in the sql result set.
                //The  allLabeledElementsAsSet undoes this duplication by ensuring that there is only one path for the object with multiple labels.
                Map<String, Set<Object>> allLabeledElementMap = new HashMap<>();
                for (String label : sortedKeys) {
                    String realLabel;
                    String pathLabel;
                    if (label.contains(BaseSqlgStrategy.PATH_LABEL_SUFFIX)) {
                        realLabel = label.substring(label.indexOf(BaseSqlgStrategy.PATH_LABEL_SUFFIX) + BaseSqlgStrategy.PATH_LABEL_SUFFIX.length());
                        pathLabel = label.substring(0, label.indexOf(BaseSqlgStrategy.PATH_LABEL_SUFFIX) + BaseSqlgStrategy.PATH_LABEL_SUFFIX.length());
                    } else if (label.contains(BaseSqlgStrategy.EMIT_LABEL_SUFFIX)) {
                        realLabel = label.substring(label.indexOf(BaseSqlgStrategy.EMIT_LABEL_SUFFIX) + BaseSqlgStrategy.EMIT_LABEL_SUFFIX.length());
                        pathLabel = label.substring(0, label.indexOf(BaseSqlgStrategy.EMIT_LABEL_SUFFIX) + BaseSqlgStrategy.EMIT_LABEL_SUFFIX.length());
                    } else {
                        throw new IllegalStateException("label must contain " + BaseSqlgStrategy.PATH_LABEL_SUFFIX + " or " + BaseSqlgStrategy.EMIT_LABEL_SUFFIX);
                    }

                    Collection<Emit<E>> emits = labeledElements.get(label);
                    for (Emit<E> emit : emits) {
                        E e = emit.getElementPlusEdgeId().getLeft();
                        //This is to avoid emitting the element twice.
                        //Or if its not labeled but last then it needs to be emitted.
                        if (addElement && e.equals(element)) {
                            addElement = false;
                        }
                        Set<Object> allLabeledElementsAsSet = allLabeledElementMap.get(pathLabel);
                        if (allLabeledElementsAsSet == null) {
                            allLabeledElementsAsSet = new HashSet<>();
                            allLabeledElementMap.put(pathLabel, allLabeledElementsAsSet);
                        }
                        if (!allLabeledElementsAsSet.contains(e)) {
                            this.currentPath = this.currentPath.extend(e, Collections.singleton(realLabel));
                            allLabeledElementsAsSet.add(e);
                            if (pathLabel.endsWith(BaseSqlgStrategy.EMIT_LABEL_SUFFIX)) {
                                emit.setPath(this.currentPath.clone());
                                emit.setUseCurrentEmitTree(true);
                                flattenedEmit.add(emit);
                            }
                            lastEmit = emit;
                        } else {
                            //this adds the label to the path
                            this.currentPath = this.currentPath.extend(Collections.singleton(realLabel));
                        }
                    }
                }
                if (addElement && !(element instanceof Dummy)) {
                    //tp relies on all elements traversed being on the path.
                    //if the element is not labelled put it on the path
                    this.currentPath = this.currentPath.clone().extend(element, Collections.emptySet());
                }

//                if (lastEmit != null && lastEmit.emitAndUntilBothAtEnd()) {
                //do nothing, nothing to emit
                //If I remember correctly this is because the element will be emitted anyhow as it will have an emit 'E' label.
//                } else if (lastEmit != null) {
                if (lastEmit != null) {
                    lastEmit.setPath(this.currentPath.clone());
                    flattenedEmit.add(lastEmit);
                } else {
                    Emit<E> emit = new Emit<>(Pair.of(element, Optional.empty()), false, false, false);
                    emit.setPath(this.currentPath.clone());
                    flattenedEmit.add(emit);
                }

            } else {
                Emit<E> emit = new Emit<>(Pair.of(element, Optional.empty()), false, false, false);
                flattenedEmit.add(emit);
            }
        }
        return flattenedEmit;
    }

    @Override
    public Iterator<Emit<E>> get() {
        return this;
    }

    private boolean rootEmitTreeContains(List<EmitTree<E>> rootEmitTrees, Emit emit) {
        for (EmitTree<E> rootEmitTree : rootEmitTrees) {

            if (rootEmitTree.getEmit().getPath() == null && emit.getPath() == null) {

                if (rootEmitTree.getEmit().getElementPlusEdgeId().equals(emit.getElementPlusEdgeId())) {
                    return true;
                }

            } else if (rootEmitTree.getEmit().getPath().get(0).equals(emit.getPath().get(0)) &&
                    rootEmitTree.getEmit().getElementPlusEdgeId().equals(emit.getElementPlusEdgeId())) {

                return true;
            }
        }
        return false;
    }
}
