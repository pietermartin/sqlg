package org.umlg.sqlg.structure;

/**
 * Created by pieter on 2014/09/24.
 */
@FunctionalInterface
interface ElementPropertyRollback {
    void clearProperties();
}
