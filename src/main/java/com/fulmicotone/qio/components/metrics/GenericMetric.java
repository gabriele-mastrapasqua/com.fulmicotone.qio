package com.fulmicotone.qio.components.metrics;

import com.fulmicotone.qio.components.metrics.interfaces.IMetric;
import com.netflix.servo.annotations.DataSourceLevel;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Map;

public abstract class GenericMetric<I> implements IMetric<I> {

    private final Logger log = LoggerFactory.getLogger(getClass());


    protected String name;
    protected DataSourceType type;
    protected String desc;
    protected DataSourceLevel level;


    public GenericMetric(String name, DataSourceType type, String desc, DataSourceLevel level)
    {
        this.name = name;
        this.type = type;
        this.desc = desc;
        this.level = level;
    }

    @Override
    public String toString() {
        return "Metric: "+name+" of type "+type.getValue()+" at level "+level.getValue();
    }

    public void register(String nameSpace) {



        try {

            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (field.isAnnotationPresent(Monitor.class)) {
                    field.setAccessible(true); // should work on private fields
                    try {
                        changeAnnotationValue(field.getAnnotation(Monitor.class), "name", name);
                        changeAnnotationValue(field.getAnnotation(Monitor.class), "level", level);
                        changeAnnotationValue(field.getAnnotation(Monitor.class), "type", type);
                        changeAnnotationValue(field.getAnnotation(Monitor.class), "description", desc);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            log.info("METRIC: Register :"+this.toString());

            Monitors.registerObject(nameSpace, this);


        } catch (Exception e) {
            e.printStackTrace();
            log.error("Exception in register metric: "+e.getMessage());

        }

    }

    private static void changeAnnotationValue(Annotation annotation, String key, Object newValue){
        Object handler = Proxy.getInvocationHandler(annotation);
        Field f;
        try {
            f = handler.getClass().getDeclaredField("memberValues");
        } catch (NoSuchFieldException | SecurityException e) {
            throw new IllegalStateException(e);
        }
        f.setAccessible(true);
        Map<String, Object> memberValues;
        try {
            memberValues = (Map<String, Object>) f.get(handler);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        Object oldValue = memberValues.get(key);
        if (oldValue == null || oldValue.getClass() != newValue.getClass()) {
            throw new IllegalArgumentException();
        }
        memberValues.put(key,newValue);
    }
}
