package org.umlg.sqlg.ui.util;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectMapperFactory {
    public static final ObjectMapperFactory INSTANCE = new ObjectMapperFactory();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private ObjectMapperFactory() {
    }

    public ObjectMapper getObjectMapper() {
        return this.objectMapper;
    }
}
