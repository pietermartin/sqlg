package org.umlg.sqlgraph.h2database.test;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.umlg.sqlgraph.sql.impl.SqlGraphDataSource;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public abstract class BaseTest {

    protected static Path testDir = Paths.get("src/test/h2database");
    protected static final String JDBC_DRIVER = "org.h2.Driver";
    protected static final String DB_URL = "jdbc:h2:" + testDir.toAbsolutePath().toString() + "/test";

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        SqlGraphDataSource.INSTANCE.setupDataSource(DB_URL);
    }

    @Before
    public void before() throws IOException {
        FileUtils.cleanDirectory(testDir.toFile());
    }
}
