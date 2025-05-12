package org.umlg.sqlg.sql.parse;

import java.util.function.Function;

public record SqlgFunctionConfig(Function<Object, String> function) {

}
