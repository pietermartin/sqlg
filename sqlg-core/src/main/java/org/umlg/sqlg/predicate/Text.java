package org.umlg.sqlg.predicate;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

/**
 * Created by pieter on 2015/08/12.
 */
public enum Text implements BiPredicate<String, String> {

    eq {
        @Override
        public boolean test(final String first, final String second) {
            Preconditions.checkState(first != null && second != null, "Test.eq may not be called with a null value.");
           return first.contains(second);
        }

        @Override
        public Text negate() {
            return neq;
        }
    }, neq {
        @Override
        public boolean test(final String first, final String second) {
            return !eq.test(first, second);
        }

        @Override
        public Text negate() {
            return eq;
        }
    };

    public static P<String> eq(final String value) {
        return new P(Text.eq, value);
    }
}
