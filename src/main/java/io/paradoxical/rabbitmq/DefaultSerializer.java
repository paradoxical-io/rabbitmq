package io.paradoxical.rabbitmq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk7.Jdk7Module;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.xebia.jacksonlombok.JacksonLombokAnnotationIntrospector;

import java.io.IOException;

public class DefaultSerializer implements QueueSerializer {

    private ObjectMapper mapper;

    public DefaultSerializer() {
        this(initMapper());
    }

    private static ObjectMapper initMapper() {
        return new ObjectMapper()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(SerializationFeature.INDENT_OUTPUT, true)
                .configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true)
                .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
                .setAnnotationIntrospector(new JacksonLombokAnnotationIntrospector())
                .registerModule(new JodaModule())
                .registerModule(new GuavaModule())
                .registerModule(new Jdk7Module())
                .registerModule(new Jdk8Module());
    }

    public DefaultSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public <T> T read(final byte[] bytes, final Class<T> clazz) throws IOException {
        return mapper.readValue(bytes, clazz);
    }

    @Override
    public <T> T read(final String json, final Class<T> clazz) throws Exception {
        return mapper.readValue(json, clazz);
    }

    @Override
    public <T> byte[] writeBytes(final T item) throws JsonProcessingException {
        return mapper.writeValueAsBytes(item);
    }

    @Override
    public <T> String writeString(final T item) throws JsonProcessingException {
        return mapper.writeValueAsString(item);
    }
}
