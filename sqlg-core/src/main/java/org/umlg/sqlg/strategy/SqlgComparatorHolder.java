package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 *         Date: 2017/03/07
 */
public class SqlgComparatorHolder {

    private String precedingSelectOneLabel;
    private List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators = new ArrayList<>();

    public SqlgComparatorHolder() {
    }

    String getPrecedingSelectOneLabel() {
        return precedingSelectOneLabel;
    }

    void setPrecedingSelectOneLabel(String precedingSelectOneLabel) {
        this.precedingSelectOneLabel = precedingSelectOneLabel;
    }

    boolean hasPrecedingSelectOneLabel() {
        return this.precedingSelectOneLabel != null;
    }

    public void setComparators(List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators) {
        this.comparators = comparators;
    }

    public List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> getComparators() {
        return this.comparators;
    }

    public boolean hasComparators() {
        return !this.comparators.isEmpty();
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof SqlgComparatorHolder other)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        return Objects.equals(this.precedingSelectOneLabel, other.precedingSelectOneLabel) &&
                Objects.equals(this.comparators, other.comparators);
    }
}
