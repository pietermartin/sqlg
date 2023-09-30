package org.umlg.sqlg.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Postgres specific array data type operator to check if an array is fully contained in another.
 * <a href="https://www.postgresql.org/docs/9.6/functions-array.html">functions-array</a>
 */
public record ArrayContains<T>(T[] values) implements PBiPredicate<T[], T[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrayContains.class);

    public ArrayContains(T[] values) {
        Set<T> uniqueValues = Arrays.stream(values).collect(Collectors.toSet());
        this.values = uniqueValues.toArray(values);
    }

    public P<T[]> getPredicate() {
        return new P<>(this, values);
    }

    @Override
    public boolean test(T[] lhs, T[] rhs) {
        LOGGER.warn("Using Java implementation of @> (array contains) instead of database");
        Set<T> lhsSet = new HashSet<>(Arrays.asList(lhs));

        for (T item : rhs) {
            if (!lhsSet.contains(item)) {
                return false;
            }
        }

        return true;
    }
}
