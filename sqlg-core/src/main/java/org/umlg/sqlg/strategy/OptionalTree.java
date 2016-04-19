package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2016/04/18
 * Time: 7:40 PM
 */
public class OptionalTree<E extends SqlgElement> {

    private Emit<E> emit;
    private List<OptionalTree> children = new ArrayList<>();

    public OptionalTree(Emit<E> emit) {
        this.emit = emit;
    }

    public Emit<E> getEmit() {
        return emit;
    }

    public OptionalTree<E> addEmit(Emit<E> emit) {
        OptionalTree child = new OptionalTree(emit);
        this.children.add(child);
        return child;
    }

    public boolean hasChild(Pair elementPlusEdgeId) {
        if (this.children.isEmpty()) {
            return false;
        } else {
            for (OptionalTree child : children) {
                if (child.emit.getPath().equals(elementPlusEdgeId)) {
                    return true;
                }
            }
            return false;
        }
    }
}
