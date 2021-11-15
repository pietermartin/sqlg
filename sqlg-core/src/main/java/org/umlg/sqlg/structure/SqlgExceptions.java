package org.umlg.sqlg.structure;

/**
 * Date: 2015/02/21
 * Time: 8:56 PM
 */
public class SqlgExceptions {

    private static final String BATCH_MODE_NOT_SUPPORTED = "Batch processing is not supported by %s";
    private static final String MULTIPLE_JVM_NOT_SUPPORTED = "Multiple jvm(s) is not supported by %s";
    private static final String TOPOLOGY_RENAME_NOT_SUPPORTED = "Topology rename is not supported by %s";

    private SqlgExceptions() {}

    public static UnsupportedOperationException topologyRenameNotSupported(String type) {
        return new UnsupportedOperationException(String.format(TOPOLOGY_RENAME_NOT_SUPPORTED, type));
    }

    public static UnsupportedOperationException multipleJvmNotSupported(String dialect) {
        return new UnsupportedOperationException(String.format(MULTIPLE_JVM_NOT_SUPPORTED, dialect));
    }

    public static UnsupportedOperationException batchModeNotSupported(String dialect) {
        return new UnsupportedOperationException(String.format(BATCH_MODE_NOT_SUPPORTED, dialect));
    }

    public static TopologyLockTimeout topologyLockTimeout(String message) {
        return new TopologyLockTimeout(message);
    }

    public static WriteLockTimeout writeLockTimeout(String message) {
        return new WriteLockTimeout(message);
    }

    public static DeadLockDetected deadLockDetected(String message) {
        return new DeadLockDetected(message);
    }

    static InvalidIdException invalidId(String invalidId) {
        return new InvalidIdException("Sqlg ids must be a String.\nFor sequence ids the format is 'label:::id' The id must be a long.\n" +
                "For user supplied identifiers the identifiers must be in array format. i.e. [x,y,z].\n" +
                "The given id " + invalidId + " is invalid.");
    }

    public static InvalidSchemaException invalidSchemaName(String message) {
        return new InvalidSchemaException(message);
    }

    public static InvalidTableException invalidTableName(String message) {
        return new InvalidTableException(message);
    }

    public static InvalidColumnException invalidColumnName(String message) {
        return new InvalidColumnException(message);
    }

    public static IllegalStateException invalidMode(String message) {
        return new IllegalStateException(message);
    }

    public static InvalidPropertyTypeException invalidPropertyType(PropertyType propertyType) {
        return new InvalidPropertyTypeException("Property of type " + propertyType.name() + " is not supported");
    }

    public static GisNotSupportedException gisNotSupportedException(PropertyType propertyType) {
        return new GisNotSupportedException("Gis property of type " + propertyType.name() + " is not supported");
    }

    public static GisNotSupportedException gisNotSupportedException() {
        return new GisNotSupportedException("Gis is not supported");
    }

    public static InvalidFromRecordIdException invalidFromRecordId(String elementId) {
        throw new InvalidFromRecordIdException(String.format("To convert a String representation of an id for an user supplied identifier element use RecordId.from(SqlgGraph, Object). Given id is %s", elementId));
    }

    public static class WriteLockTimeout extends RuntimeException {

        WriteLockTimeout(String message) {
            super(message);
        }

    }

    public static class TopologyLockTimeout extends RuntimeException {

        TopologyLockTimeout(String message) {
            super(message);
        }

    }

    public static class DeadLockDetected extends RuntimeException {

        DeadLockDetected(String message) {
            super(message);
        }

    }

    public static class InvalidFromRecordIdException extends RuntimeException {

        InvalidFromRecordIdException(String message) {
            super(message);
        }

    }

    public static class InvalidIdException extends RuntimeException {

        InvalidIdException(String message) {
            super(message);
        }

    }

    public static class InvalidSchemaException extends RuntimeException {

        InvalidSchemaException(String message) {
            super(message);
        }

    }

    public static class InvalidTableException extends RuntimeException {

        InvalidTableException(String message) {
            super(message);
        }

    }

    public static class InvalidColumnException extends RuntimeException {

        InvalidColumnException(String message) {
            super(message);
        }

    }

    public static class InvalidPropertyTypeException extends RuntimeException {

        InvalidPropertyTypeException(String message) {
            super(message);
        }

    }

    public static class GisNotSupportedException extends UnsupportedOperationException {

        GisNotSupportedException(String message) {
            super(message);
        }

    }

}
