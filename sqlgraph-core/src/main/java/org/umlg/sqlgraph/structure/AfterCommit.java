package org.umlg.sqlgraph.structure;

/**
 * Date: 2014/07/17
 * Time: 7:34 AM
 */
@FunctionalInterface
public interface AfterCommit {

     void doAfterCommit();

}
