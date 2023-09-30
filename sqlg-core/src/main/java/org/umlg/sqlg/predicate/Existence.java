package org.umlg.sqlg.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

/**
 * abuse BiPredicate to represent nulls
 *
 * @author JP Moresmau
 */
public enum Existence implements PBiPredicate<String, String> {
    NULL {
        @Override
        public boolean test(String t, String u) {
            return t == null || t.length() == 0;
        }

        @Override
        public String toString() {
            return "IS NULL";
        }
    },
    NOTNULL {
        @Override
        public boolean test(String t, String u) {
            return t != null && t.length() > 0;
        }

        @Override
        public String toString() {
            return "IS NOT NULL";
        }
    }
}
