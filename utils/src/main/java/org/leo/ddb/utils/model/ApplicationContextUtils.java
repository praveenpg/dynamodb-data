package org.leo.ddb.utils.model;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"unchecked", "SameParameterValue"})
public final class ApplicationContextUtils {
    private static final ConcurrentHashMap<String, Object> BEAN_NAME_TO_BEAN_MAPPING = new ConcurrentHashMap<>();
    private static ApplicationContext CONTEXT;
    private static Environment environment;

    private ApplicationContextUtils() {
    }
    public static <T> T getBean(final String name) {
        return (T) BEAN_NAME_TO_BEAN_MAPPING.computeIfAbsent(name, CONTEXT::getBean);
    }

    public static <T> T getBean(final Class<T> type) {
        return (T) BEAN_NAME_TO_BEAN_MAPPING.computeIfAbsent(type.getName(), name -> CONTEXT.getBean(type));
    }

    public static <T> T getBean(final Class<T> type, final String qualifierName) {
        return (T) BEAN_NAME_TO_BEAN_MAPPING.computeIfAbsent(type.getName() + ":" + qualifierName, name -> CONTEXT.getBean(qualifierName, type));
    }

    public static void setApplicationContext(final ApplicationContext applicationContext) {
        if(CONTEXT == null) {
            synchronized (ApplicationContextUtils.class) {
                if(CONTEXT == null) {
                    CONTEXT = applicationContext;
                }
            }
        }
    }

    public static void setEnvironment(final Environment environment) {
        if(ApplicationContextUtils.environment == null) {
            synchronized (ApplicationContextUtils.class) {
                if(ApplicationContextUtils.environment == null) {
                    ApplicationContextUtils.environment = environment;
                }
            }
        }
    }

    public static Environment getEnvironment() {
        return environment;
    }
}
