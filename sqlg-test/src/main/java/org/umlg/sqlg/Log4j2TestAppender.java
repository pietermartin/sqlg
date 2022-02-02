package org.umlg.sqlg;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.util.LinkedList;

@Plugin(
        name = "Log4j2TestAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE)
public class Log4j2TestAppender extends AbstractAppender {

    private static final LinkedList<Pair<LogEvent, String>> EVENT_LIST = new LinkedList<>();

    public static String last(String name) {
        synchronized (EVENT_LIST) {
            if (EVENT_LIST.isEmpty()) {
                return null;
            }
            Pair<LogEvent, String> event = EVENT_LIST.removeLast();
            while (event != null && event.getLeft().getLoggerName() != null && !event.getLeft().getLoggerName().equals(name)) {
                event = EVENT_LIST.removeLast();
            }
            EVENT_LIST.clear();
            if (event != null) {
                return event.getRight();
            } else {
                return null;
            }
        }
    }

    protected Log4j2TestAppender(String name, Filter filter) {
        super(name, filter, null, true, null);
    }

    @SuppressWarnings("unused")
    @PluginFactory
    public static Log4j2TestAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") Filter filter) {
        return new Log4j2TestAppender(name, filter);
    }

    @Override
    public void append(LogEvent event) {
        synchronized (EVENT_LIST) {
            EVENT_LIST.add(Pair.of(event, event.getMessage().getFormattedMessage()));
            // keep memory low, since we want the last event usually anyway
            if (EVENT_LIST.size() > 10) {
                EVENT_LIST.removeFirst();
            }
        }
    }
}
