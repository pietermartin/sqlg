package org.umlg.sqlg.util;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Date: 2015/05/27
 * Time: 7:33 PM
 */
public class StreamFactory {

    private StreamFactory() {}

    public  static <T> Stream<T> stream(Iterator<T> iter) {
        Iterable<T> iterable = () -> iter;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

}
