package io.paradoxical.rabbitmq;

import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Optional;

@Data
public class Queue {
    public static final String ANONYMOUS_NAME = "";

    @Getter
    @Setter(AccessLevel.PRIVATE)
    private QueueOptions options = new QueueOptions();

    private final String name;

    /**
     * The name used in RMQ
     */
    private String declaredName;

    public Queue getRetryQueue(){
        return clone(name + "-wait");
    }

    private Optional<List<String>> routingKeys;

    public Queue(String name, Optional<List<String>> routingKeys) {
        this.name = name;
        this.routingKeys = routingKeys;
    }

    public static Queue valueOf(String name) {
        return new Queue(name, Optional.empty());
    }

    public Queue withRoute(String route) {
        return withRoutes(route);
    }

    public Queue withOptions(QueueOptions options){
        this.options = options;

        return this;
    }

    public Queue clone(String newName){
        return new Queue(newName, routingKeys).withOptions(options);
    }

    public Queue withRoutes(String... routes) {
        List<String> routeList = Lists.newArrayList(routes);
        if(routingKeys.isPresent()) {
            routingKeys.get().addAll(routeList);
        }
        else {
            routingKeys = Optional.of(routeList);
        }
        return this;
    }
}
