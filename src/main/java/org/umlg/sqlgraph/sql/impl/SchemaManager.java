package org.umlg.sqlgraph.sql.impl;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Date: 2014/07/12
 * Time: 11:02 AM
 */
public class SchemaManager {

    public static final String VERTICES = "VERTICES";
    public static final String EDGES = "EDGES";
    public static final SchemaManager INSTANCE = new SchemaManager();

    public enum PropertyType {
        BOOLEAN("BOOLEAN", Boolean.class.getName()),
        BYTE("TINYINT", Byte.class.getName()),
        SHORT("SMALLINT", Short.class.getName()),
        INTEGER("INT", Integer.class.getName()),
        LONG("BIGINT", Long.class.getName()),
        FLOAT("REAL", Float.class.getName()),
        DOUBLE("DOUBLE", Double.class.getName()),
        STRING("VARCHAR", String.class.getName());
        //TODO this should go through a dialect alla hibernate
        private String dbName;
        private String javaClassName;
        private static final Map<String, PropertyType> javaClassNameToEnum = new HashMap<>();

        private PropertyType(String dbName, String javaClassName) {
            this.dbName = dbName;
            this.javaClassName = javaClassName;
        }

        static {
            for (PropertyType pt : values()) {
                javaClassNameToEnum.put(pt.javaClassName, pt);
            }
        }

        public String getDbName() {
            return dbName;
        }

        public static PropertyType from(Object o) {
            return javaClassNameToEnum.get(o.getClass().getName());
        }

    }

    private Map<String, List<ImmutablePair<String, PropertyType>>> schema = new ConcurrentHashMap<>();
    private boolean createdGlobalVertices = false;
    private boolean createdGlobalEdges = false;

    private SchemaManager() {
    }

    /**
     * This table holds a reference to every vertex.
     * This is to help implement g.v(id) and g.V()
     */
    public void ensureGlobalVerticesTableExist() {
        //TODO getOrCreate concurrent code here
        if (this.createdGlobalVertices) {
            //Table exist, check columns exist
        } else {
            SchemaCreator.INSTANCE.createVerticesTable();
            this.createdGlobalVertices = true;
        }
    }

    /**
     * This table holds a reference to every edge.
     * This is to help implement g.e(id) and g.E()
     */
    public void ensureGlobalEdgesTableExist() {
        //TODO getOrCreate concurrent code here
        if (this.createdGlobalEdges) {
            //Table exist, check columns exist
        } else {
            SchemaCreator.INSTANCE.createEdgesTable();
            this.createdGlobalEdges = true;
        }
    }

    public void ensureVertexTableExist(String table, Object... keyValues) {
        //TODO getOrCreate concurrent code here
        List<ImmutablePair<String, SchemaManager.PropertyType>> columns = this.schema.get(table);
        if (columns != null) {
            //Table exist, check columns exist
        } else {
            columns = SqlUtil.transformToColumnDefinition(keyValues);
            SchemaCreator.INSTANCE.createVertexTable(table, columns);
            this.schema.put(table, columns);
        }
    }

    public void ensureEdgeTableExist(String table, String inTable, String outTable, Object... keyValues) {
        //TODO getOrCreate concurrent code here
        List<ImmutablePair<String, SchemaManager.PropertyType>> columns = this.schema.get(table);
        if (columns != null) {
            //Table exist, check columns exist
        } else {
            columns = SqlUtil.transformToColumnDefinition(keyValues);
            SchemaCreator.INSTANCE.createEdgeTable(table, inTable, outTable, columns);
            this.schema.put(table, columns);
        }
    }

}
