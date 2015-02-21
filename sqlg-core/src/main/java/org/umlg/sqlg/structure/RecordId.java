package org.umlg.sqlg.structure;

/**
 * Date: 2015/02/21
 * Time: 8:50 PM
 */
public class RecordId {

    public final static String RECORD_ID_DELIMITER = ":::";
    private String label;
    private Long id;

    public static void validateId(Object vertexId) {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }
    }
}
