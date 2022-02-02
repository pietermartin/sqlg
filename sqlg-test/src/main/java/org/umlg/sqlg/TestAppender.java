package org.umlg.sqlg;

/**
 * Test appender to capture some explicit messages
 * @author JP Moresmau 
 * 
 */
public class TestAppender  {
//public class TestAppender extends AppenderSkeleton {
//	private static final LinkedList<LoggingEvent> eventsList = new LinkedList<>();
//
//	public static LinkedList<LoggingEvent> getEventsList() {
//		return eventsList;
//	}
//
//	/**
//	 * get last event of the given name AND CLEARS THE LIST
//	 *
//	 * @param name the event name (class where event was logged)
//	 * @return the event or null if none
//	 */
//	public static LoggingEvent getLast(String name){
//		synchronized (eventsList) {
//			if (eventsList.isEmpty()){
//				return null;
//			}
//			LoggingEvent evt=eventsList.removeLast();
//			while (evt!=null && !evt.getLoggerName().equals(name)){
//				evt=eventsList.removeLast();
//			}
//			eventsList.clear();
//			return evt;
//		}
//	}
//
//	public TestAppender() {
//
//	}
//
//	public TestAppender(boolean isActive) {
//		super(isActive);
//	}
//
//	@Override
//	public void close() {
//
//	}
//
//	@Override
//	public boolean requiresLayout() {
//		return false;
//	}
//
//	@Override
//	protected void append(LoggingEvent event) {
//		synchronized (eventsList) {
//			eventsList.add(event);
//			// keep memory low, since we want the last event usually anyway
//			if (eventsList.size()>10){
//				eventsList.removeFirst();
//			}
//		}
//	}

}
