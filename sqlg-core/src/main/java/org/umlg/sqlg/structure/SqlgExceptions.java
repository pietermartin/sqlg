package org.umlg.sqlg.structure;

/**
 * Date: 2015/02/21
 * Time: 8:56 PM
 */
public class SqlgExceptions {

    private SqlgExceptions() {}

    public static InvalidIdException invalidId(String invalidId) {
        return new InvalidIdException("Sqlg ids must be a String with the format 'label:::id' The id must be a long. The given id " + invalidId + " is invalid.");
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

    public static UniqueConstraintViolationException uniqueConstraintViolation(String key, Object value) {
        return new UniqueConstraintViolationException("Unique value '" + value.toString() + "' for property '" + key
                + "' already exists in the database.");
    }

    public static class InvalidIdException extends RuntimeException {

        public InvalidIdException(String message) {
            super(message);
        }

    }

    public static class InvalidSchemaException extends RuntimeException {

        public InvalidSchemaException(String message) {
            super(message);
        }

    }

    public static class InvalidTableException extends RuntimeException {

        public InvalidTableException(String message) {
            super(message);
        }

    }

    public static class InvalidColumnException extends RuntimeException {

        public InvalidColumnException(String message) {
            super(message);
        }

    }

    public static class InvalidPropertyTypeException extends RuntimeException {

        public InvalidPropertyTypeException(String message) {
            super(message);
        }

    }

    public static class UniqueConstraintViolationException extends RuntimeException {

        public UniqueConstraintViolationException(String message) {
            super(message);
        }

    }
}
