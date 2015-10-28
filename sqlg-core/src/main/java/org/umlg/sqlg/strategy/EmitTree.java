package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pieter on 2015/10/26.
 */
public class EmitTree<E extends SqlgElement> {

    private int degree;
    private Emit<E> emit;
    private List<EmitTree> children = new ArrayList<>();
    private EmitTree parent = null;
    private EmitTree<E> root;

    public EmitTree(int degree, Emit<E> emit) {
        this.degree = degree;
        this.emit = emit;
    }

    public Emit<E> getEmit() {
        return emit;
    }

    public EmitTree getParent() {
        return parent;
    }

    public EmitTree<E> addEmit(int degree, Emit<E> emit) {
        EmitTree child = new EmitTree(degree, emit);
        child.parent = this;
        this.children.add(child);
        return child;
    }

    public boolean emitEquals(E e) {
        return this.emit.getElementPlusEdgeId().getLeft().equals(e);
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
                if (child.emit.getElementPlusEdgeId().equals(elementPlusEdgeId)) {
                    return true;
                }
            }
            return false;
        }
    }

    public EmitTree<E> getChild(Pair elementPlusEdgeId) {
        for (EmitTree child : children) {
            if (child.emit.getElementPlusEdgeId().equals(elementPlusEdgeId)) {
                return child;
            }
        }
        return null;
    }
}
