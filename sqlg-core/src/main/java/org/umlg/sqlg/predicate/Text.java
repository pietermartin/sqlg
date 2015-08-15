package org.umlg.sqlg.predicate;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

/**
 * Created by pieter on 2015/08/12.
 */
public enum Text implements BiPredicate<String, String> {

    contains {
        @Override
        public boolean test(final String first, final String second) {
            Preconditions.checkState(first != null && second != null, "Test.contains may not be called with a null value.");
            return first.contains(second);
        }

        @Override
        public Text negate() {
            return ncontains;
        }
    }, ncontains {
        @Override
        public boolean test(final String first, final String second) {
            return !contains.test(first, second);
        }

        @Override
        public Text negate() {
            return contains;
        }
    }, containsCIS {
        @Override
        public boolean test(final String first, final String second) {
            return contains.test(first.toLowerCase(), second.toLowerCase());
        }

        @Override
        public Text negate() {
            return ncontains;
        }
    }, ncontainsCIS {
        @Override
        public boolean test(final String first, final String second) {
            return !contains.test(first.toLowerCase(), second.toLowerCase());
        }

        @Override
        public Text negate() {
            return containsCIS;
        }
    }, startsWith{
        @Override
        public boolean test(final String first, final String second) {
            return first.startsWith(second);
        }

        @Override
        public Text negate() {
            return nstartsWith;
        }
    }, nstartsWith{
        @Override
        public boolean test(final String first, final String second) {
            return !first.startsWith(second);
        }

        @Override
        public Text negate() {
            return startsWith;
        }
    }, endsWith{
        @Override
        public boolean test(final String first, final String second) {
            return first.startsWith(second);
        }

        @Override
        public Text negate() {
            return nendsWith;
        }
    }, nendsWith{
        @Override
        public boolean test(final String first, final String second) {
            return !first.startsWith(second);
        }

        @Override
        public Text negate() {
            return endsWith;
        }
    };

    public static P<String> contains(final String value) {
        return new P(Text.contains, value);
    }

    public static P<String> ncontains(final String value) {
        return new P(Text.ncontains, value);
    }

    public static P<String> containsCIS(final String value) {
        return new P(Text.containsCIS, value);
    }

    public static P<String> ncontainsCIS(final String value) {
        return new P(Text.ncontainsCIS, value);
    }

    public static P<String> startsWith(final String value) {
        return new P(Text.startsWith, value);
    }

    public static P<String> nstartsWith(final String value) {
        return new P(Text.nstartsWith, value);
    }

    public static P<String> endsWith(final String value) {
        return new P(Text.endsWith, value);
    }

    public static P<String> nendsWith(final String value) {
        return new P(Text.nendsWith, value);
    }
}
