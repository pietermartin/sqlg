package org.umlg.sqlg.structure;

/**
 * Date: 2014/07/17
 * Time: 7:34 AM
 */
@FunctionalInterface
public interface AfterRollback {

     void doAfterRollback();

}
