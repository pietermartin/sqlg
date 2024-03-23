module sqlg.core {
    
    requires transitive sqlg.tinkerpop.shaded;

    //start for intellij only
    requires gremlin.core;
    requires gremlin.shaded;
    requires org.apache.commons.configuration2;
    requires org.apache.commons.text;
    requires javatuples;
    //end for intellij only

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

    exports org.umlg.sqlg.sql.parse to sqlg.postgres.dialect;
    exports org.umlg.sqlg.strategy to sqlg.postgres.dialect;

    uses org.umlg.sqlg.SqlgPlugin;
}
