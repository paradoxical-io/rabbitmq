package io.paradoxical.rabbitmq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import java.io.IOException;

public class DefaultSerializer implements QueueSerializer {

    private ObjectMapper mapper;

    public DefaultSerializer() {
        this(initMapper());
    }

    private static ObjectMapper initMapper() {
        return new ObjectMapper().configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                                 .configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true)
                                 .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                                 .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
                                 .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                                 .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                                 .configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true)
                                 .registerModule(new JodaModule())
                                 .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    }

    public DefaultSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override public <T> T read(final byte[] bytes, final Class<T> clazz) throws IOException {
        return mapper.readValue(bytes, clazz);
    }

    @Override public <T> T read(final String json, final Class<T> clazz) throws Exception {
        return mapper.readValue(json, clazz);
    }

    @Override public <T> byte[] writeBytes(final T item) throws JsonProcessingException {
        return mapper.writeValueAsBytes(item);
    }

    @Override public <T> String writeString(final T item) throws JsonProcessingException {
        return mapper.writeValueAsString(item);
    }
}
