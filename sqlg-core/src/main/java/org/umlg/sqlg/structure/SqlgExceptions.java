package org.umlg.sqlg.structure;

/**
 * Date: 2015/02/21
 * Time: 8:56 PM
 */
public class SqlgExceptions {

    public static IllegalArgumentException invalidId(String invalidId) {
        return new IllegalArgumentException("Sqlg ids must be a String with the format 'label:::id'. The given id " + invalidId + " is invalid.");
    }
}
