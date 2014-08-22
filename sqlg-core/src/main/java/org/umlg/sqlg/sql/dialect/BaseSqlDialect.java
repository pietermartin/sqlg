package org.umlg.sqlg.sql.dialect;

import org.apache.commons.configuration.Configuration;

/**
 * Date: 2014/08/21
 * Time: 6:52 PM
 */
public abstract class BaseSqlDialect implements SqlDialect {

    protected Configuration configurator;

    public BaseSqlDialect(Configuration configurator) {
        this.configurator = configurator;
    }

    public void setConfiguration(Configuration configuration) {
        this.configurator = configuration;
    }

    public Configuration getConfiguration() {
        return this.configurator;
    }

}
