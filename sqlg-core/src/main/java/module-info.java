module sqlg.core {
    
    requires transitive sqlg.tinkerpop.shaded;

    requires java.sql;
    requires java.naming;
    
    requires org.slf4j;

    requires org.apache.commons.lang3;
    requires org.apache.commons.collections4;

    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    requires com.google.common;

    exports org.umlg.sqlg;
    exports org.umlg.sqlg.util;
    exports org.umlg.sqlg.structure;
    exports org.umlg.sqlg.structure.topology;
    exports org.umlg.sqlg.sql.dialect;
    exports org.umlg.sqlg.predicate;

    exports org.umlg.sqlg.sql.parse to sqlg.postgres.dialect,sqlg.h2.dialect;
    exports org.umlg.sqlg.strategy to sqlg.postgres.dialect,sqlg.h2.dialect;

    uses org.umlg.sqlg.SqlgPlugin;
}
