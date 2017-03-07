package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/07
 */
public class SqlgComparatorHolder {

    private String precedingSelectOneLabel;
    private List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators = new ArrayList<>();

    public String getPrecedingSelectOneLabel() {
        return precedingSelectOneLabel;
    }

    public void setPrecedingSelectOneLabel(String precedingSelectOneLabel) {
        this.precedingSelectOneLabel = precedingSelectOneLabel;
    }

    public void setComparators(List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators) {
        this.comparators = comparators;
    }

    public List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> getComparators() {
        return comparators;
    }

    public boolean hasComparators() {
        return !this.comparators.isEmpty();
    }
}
