module sqlg.c3p0 {
    requires sqlg.core;

    requires java.sql;

    requires c3p0;
    requires org.slf4j;
    requires com.google.common;
    requires org.apache.commons.configuration2;
    requires org.apache.commons.lang3;

    exports org.umlg.sqlg.structure.ds.c3p0;
}
