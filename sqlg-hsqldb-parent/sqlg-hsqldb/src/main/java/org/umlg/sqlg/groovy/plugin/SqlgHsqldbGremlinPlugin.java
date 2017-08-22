package org.umlg.sqlg.groovy.plugin;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;

import java.util.Set;

/**
 * Date: 2014/10/11
 * Time: 9:55 AM
 */
public class SqlgHsqldbGremlinPlugin extends AbstractGremlinPlugin {


    public SqlgHsqldbGremlinPlugin(String moduleName, Customizer... customizers) {
        super(moduleName, customizers);
    }

    public SqlgHsqldbGremlinPlugin(String moduleName, Set<String> appliesTo, Customizer... customizers) {
        super(moduleName, appliesTo, customizers);
    }

    @Override
    public String getName() {
        return "sqlg.hsqldb";
    }

}
