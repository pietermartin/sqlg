package org.umlg.sqlg;

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

    private static final LinkedList<LogEvent> eventList = new LinkedList<>();

    public static LogEvent last(String name) {
        synchronized (eventList) {
            if (eventList.isEmpty()) {
                return null;
            }
            LogEvent evt = eventList.removeLast();
            while (evt != null && evt.getLoggerName() != null && !evt.getLoggerName().equals(name)) {
                evt = eventList.removeLast();
            }
            eventList.clear();
            return evt;
        }
    }

    protected Log4j2TestAppender(String name, Filter filter) {
        super(name, filter, null, true, null);
    }

    @PluginFactory
    public static Log4j2TestAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") Filter filter) {
        return new Log4j2TestAppender(name, filter);
    }

    @Override
    public void append(LogEvent event) {
        synchronized (eventList) {
            eventList.add(event);
            // keep memory low, since we want the last event usually anyway
            if (eventList.size() > 10) {
                eventList.removeFirst();
            }
        }
    }
}
