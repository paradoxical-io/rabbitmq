package io.paradoxical.rabbitmq.tests.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.paradoxical.rabbitmq.queues.EventBase;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
        property = "type")
@JsonSubTypes({
                      @JsonSubTypes.Type(value = Subclass.class, name = "sub"),
              })

@EqualsAndHashCode(callSuper = false)
public class Data extends EventBase {
    public Data() {
    }

    public Data(long num){
        age = num;
    }

    @Getter
    @Setter
    private String name;

    @Getter
    @Setter
    private long age;
}
