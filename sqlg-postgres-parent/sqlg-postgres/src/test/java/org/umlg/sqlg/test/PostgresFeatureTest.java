package org.umlg.sqlg.test;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.cucumber.guice.CucumberModules;
import io.cucumber.guice.GuiceFactory;
import io.cucumber.guice.InjectorSource;
import io.cucumber.java.Scenario;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.junit.AssumptionViolatedException;
import org.junit.runner.RunWith;
import org.umlg.sqlg.SqlgWorld;
import org.umlg.sqlg.structure.SqlgGraph;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;

@RunWith(Cucumber.class)
@CucumberOptions(
        tags = "not @RemoteOnly and not @MultiMetaProperties and not @GraphComputerOnly and not @AllowNullPropertyValues and not @UserSuppliedVertexPropertyIds and not @UserSuppliedEdgeIds and not @UserSuppliedVertexIds and not @TinkerServiceRegistry",
        glue = {"org.apache.tinkerpop.gremlin.features"},
        objectFactory = GuiceFactory.class,
//        features = { "../../features" },
        features = {"classpath:/features"},
        plugin = {"progress", "junit:target/cucumber.xml"})
public class PostgresFeatureTest {

    private final static SqlgGraph modern = SqlgGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.MODERN)));
    private final static SqlgGraph classic = SqlgGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.CLASSIC)));
    private final static SqlgGraph sink = SqlgGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.SINK)));
    private final static SqlgGraph grateful = SqlgGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.GRATEFUL)));
    private final static SqlgGraph empty = SqlgGraph.open(new MapConfiguration(getBaseConfiguration(null)));

    static {
        readIntoGraph(modern, GraphData.MODERN);
        readIntoGraph(classic, GraphData.CLASSIC);
        readIntoGraph(sink, GraphData.SINK);
        readIntoGraph(grateful, GraphData.GRATEFUL);
    }


    private final static Set<String> OPT_IN = Set.of(
//            "g_VXv1X_hasXage_gt_30X", "g_V_hasLabelXpersonX_asXp1X_chooseXoutEXknowsX__outXknowsXX_asXp2X_selectXp1_p2X_byXnameX"
            "g_VX1X_outE_hasXweight_inside_0_06X_inV"
    );

    private final static Map<String, String> OPT_OUT = Map.of(
            "g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX", "To slow",
            "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values", "To slow",
            "g_V_repeatXoutX_timesX8X_count", "To slow",
            "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count", "To slow"
    );

    private static void readIntoGraph(final Graph graph, final GraphData graphData) {
        try {
            graph.traversal().V().drop().iterate();
            graph.tx().commit();
            final String dataFile = TestHelper.generateTempFileFromResource(graph.getClass(),
                    GryoResourceAccess.class, graphData.location().substring(graphData.location().lastIndexOf(File.separator) + 1), "", false).getAbsolutePath();
            graph.traversal().io(dataFile).read().iterate();
            graph.tx().commit();
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    private static Map<String, Object> getBaseConfiguration(final GraphData graphData) {
        return new HashMap<>() {{
            put("jdbc.url", "jdbc:postgresql://localhost:5432/" + (graphData != null ? graphData.name() : "EMPTY"));
            put("jdbc.username", "postgres");
            put("jdbc.password", "postgres");
        }};
    }

    public static class PostgresWorld extends SqlgWorld {
        public PostgresWorld() {
            super(modern, classic, sink, grateful, empty);
        }

        @Override
        public void beforeEachScenario(final Scenario scenario) {
            super.beforeEachScenario(scenario);
            if (OPT_OUT.keySet().contains(scenario.getName())) {
                throw new AssumptionViolatedException(OPT_OUT.get(scenario.getName()));
            }
            if (!OPT_IN.isEmpty() && !OPT_IN.contains(scenario.getName())) {
                throw new AssumptionViolatedException(OPT_OUT.get(scenario.getName()));
            }
        }
    }

    public static final class ServiceModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(World.class).to(PostgresWorld.class);
        }
    }

    public static final class WorldInjectorSource implements InjectorSource {
        @Override
        public Injector getInjector() {
            return Guice.createInjector(Stage.PRODUCTION, CucumberModules.createScenarioModule(), new ServiceModule());
        }
    }

}
