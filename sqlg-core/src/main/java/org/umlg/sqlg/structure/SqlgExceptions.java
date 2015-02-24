package org.umlg.sqlg.structure;

/**
 * Date: 2015/02/21
 * Time: 8:56 PM
 */
public class SqlgExceptions {

    public static InvalidIdException invalidId(String invalidId) {
        return new InvalidIdException ("Sqlg ids must be a String with the format 'label:::id' The id must be a long. The given id " + invalidId + " is invalid.");
    }

    public static class InvalidIdException extends RuntimeException {

        public InvalidIdException(String message) {
            super(message);
        }

    }
}
