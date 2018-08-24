package org.umlg.sqlg.util;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;

import java.io.Serializable;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * This is used by clients. Do not delete.
 * Date: 2016/08/29
 * Time: 11:08 AM
 */
public class SortedTree<T> extends TreeMap<T, SortedTree<T>> implements Serializable {

    @SuppressWarnings("WeakerAccess")
    public SortedTree(Comparator<? super T> comparator) {
        super(comparator);
    }

    @SuppressWarnings("WeakerAccess")
    public void addTree(final Tree<T> tree) {
        tree.forEach((k, v) -> {
            if (this.containsKey(k)) {
                this.get(k).addTree(v);
            } else {
                SortedTree<T> sortedTree = new SortedTree<>(this.comparator());
                this.put(k, sortedTree);
                sortedTree.addTree(v);
            }
        });
    }

    public static void main(String[] args) {

        Tree<String> tree = new Tree<>();

        Tree<String> aTree = new Tree<>();
        tree.put("a", aTree);
        Tree<String> bTree = new Tree<>();
        tree.put("b", bTree);

        Tree<String> abTree = new Tree<>();
        aTree.put("ab", abTree);
        Tree<String> bbTree = new Tree<>();
        bTree.put("bb", bbTree);

        System.out.println(tree);

        SortedTree<String> sortedTree = new SortedTree<>((o1, o2) -> o1.compareTo(o2) * -1);
        sortedTree.addTree(tree);

        System.out.println(sortedTree);


    }

}

