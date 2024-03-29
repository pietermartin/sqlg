module sqlg.hikari{
    requires sqlg.core;

    requires java.sql;

    requires com.zaxxer.hikari;
    requires org.slf4j;
    requires com.google.common;
    requires org.apache.commons.configuration2;
    requires org.apache.commons.lang3;

    exports org.umlg.sqlg.structure.ds.hikari;
}
