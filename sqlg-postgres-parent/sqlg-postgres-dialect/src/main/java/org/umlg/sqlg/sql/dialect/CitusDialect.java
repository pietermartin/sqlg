package org.umlg.sqlg.sql.dialect;

import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.AbstractLabel;

public interface CitusDialect {

    int getShardCount(SqlgGraph sqlgGraph, AbstractLabel label);
}
