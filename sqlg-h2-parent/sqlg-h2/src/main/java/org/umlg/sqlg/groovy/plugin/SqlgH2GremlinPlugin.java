package org.umlg.sqlg.groovy.plugin;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;

import java.util.Set;

/**
 * @author Lukas Krejci
 * @since 1.3.0
 */
public class SqlgH2GremlinPlugin extends AbstractGremlinPlugin {

    public SqlgH2GremlinPlugin(String moduleName, Customizer... customizers) {
        super(moduleName, customizers);
    }

    public SqlgH2GremlinPlugin(String moduleName, Set<String> appliesTo, Customizer... customizers) {
        super(moduleName, appliesTo, customizers);
    }

    @Override
    public String getName() {
        return "sqlg.h2";
    }

}
