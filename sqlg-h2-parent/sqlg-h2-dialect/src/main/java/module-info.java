module sqlg.h2.dialect {

    requires sqlg.core;

    requires java.sql;
    requires org.slf4j;
    requires com.h2database;

    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    requires org.apache.commons.lang3;
    requires com.google.common;

    exports org.umlg.sqlg.dialect.impl;

    provides org.umlg.sqlg.SqlgPlugin with org.umlg.sqlg.plugin.H2Plugin;
}
