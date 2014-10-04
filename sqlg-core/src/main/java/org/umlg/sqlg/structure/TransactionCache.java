package org.umlg.sqlg.structure;

import org.apache.commons.lang3.tuple.Triple;

import java.sql.Connection;
import java.util.List;

/**
 * This objects holds is scoped to the transaction threadvar
 * Date: 2014/10/04
 * Time: 3:20 PM
 */
public class TransactionCache {

    private Connection connection;
    private List<ElementPropertyRollback> elementPropertyRollbackFunctions;
    private BatchManager batchManager;
}
