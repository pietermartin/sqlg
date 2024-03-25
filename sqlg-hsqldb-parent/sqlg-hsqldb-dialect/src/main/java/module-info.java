module sqlg.hsqldb.dialect {

    requires sqlg.core;

    requires java.sql;
    requires org.slf4j;
    requires org.hsqldb;

    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    requires org.apache.commons.lang3;
    requires com.google.common;

    exports org.umlg.sqlg.sql.dialect.impl;
    exports org.umlg.sqlg.plugin;

    provides org.umlg.sqlg.SqlgPlugin with org.umlg.sqlg.plugin.HsqldbPlugin;
}
