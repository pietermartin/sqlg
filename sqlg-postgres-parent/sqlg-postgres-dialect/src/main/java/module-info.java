module sqlg.postgres.dialect {

    requires sqlg.core;
    requires gremlin.core;

    requires java.sql;
    requires org.slf4j;
    requires org.postgresql.jdbc;
    requires postgis.jdbc;

    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    requires org.apache.commons.lang3;
    requires org.apache.commons.text;
    requires com.google.common;

    exports org.umlg.sqlg.dialect.impl;
    exports org.umlg.sqlg.plugin;
    exports org.umlg.sqlg.gis;

    provides org.umlg.sqlg.SqlgPlugin with org.umlg.sqlg.plugin.PostgresPlugin;

}
