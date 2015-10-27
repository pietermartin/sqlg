package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by pieter on 2015/10/26.
 */
public class EmitTree<E extends SqlgElement> {

    private int degree;
    private Pair<E, Optional<Long>> emit;
    private List<EmitTree> children = new ArrayList<>();
    private EmitTree parent = null;
    private EmitTree<E> root;

    public EmitTree(int degree, Pair<E, Optional<Long>> elementPlusEdgeId) {
        this.degree = degree;
        this.emit = elementPlusEdgeId;
    }

    public Pair<E, Optional<Long>> getEmit() {
        return emit;
    }

    public EmitTree getParent() {
        return parent;
    }

    public boolean anyChildEquals(Pair<E, Optional<Long>> emit) {
        for (EmitTree child : this.children) {
            if (child.emit.equals(emit)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsAtDegree(int degree, Pair<E, Optional<Long>> elementPlusEdgeId) {
        return getAtDegree(degree, elementPlusEdgeId) != null;
    }

    public EmitTree<E> getAtDegree(int degree, Pair<E, Optional<Long>> elementPlusEdgeId) {
        if (this.degree > degree) {
            return null;
        } else if (this.degree < degree) {
            for (EmitTree child : this.children) {
                return child.getAtDegree(degree, elementPlusEdgeId);
            }
        } else {
            for (EmitTree child : this.children) {
                if (child.emit.equals(elementPlusEdgeId)) {
                    return child;
                }
            }
        }
        return null;
    }

    public EmitTree<E> addEmit(int degree, Pair<E, Optional<Long>> emit) {
        EmitTree child = new EmitTree(degree, emit);
        child.parent = this;
        this.children.add(child);
        return child;
    }

    public boolean emitEquals(E e) {
        return this.emit.getLeft().equals(e);
    }

    public EmitTree<E> getRoot() {
        if (this.parent == null) {
            return this;
        } else {
            return this.parent.getRoot();
        }
    }

    public boolean isRoot() {
        return this.parent != null;
    }

    public EmitTree<E> getLastChildForDegree(int degree) {
        if (this.degree == (degree - 1)) {
            if (this.children.isEmpty()) {
                return null;
            } else {
                return this.children.get(this.children.size() - 1);
            }
        } else {
            for (EmitTree child : this.children) {
                return child.getLastChildForDegree(degree);
            }
        }
        return null;
    }

    public boolean hasChild(Pair elementPlusEdgeId) {
        if (this.children.isEmpty()) {
            return false;
        } else {
            for (EmitTree child : children) {
                if (child.emit.equals(elementPlusEdgeId)) {
                    return true;
                }
            }
            return false;
        }
    }

    public EmitTree<E> getChild(Pair elementPlusEdgeId) {
        for (EmitTree child : children) {
            if (child.emit.equals(elementPlusEdgeId)) {
                return child;
            }
        }
        return null;
    }
}
