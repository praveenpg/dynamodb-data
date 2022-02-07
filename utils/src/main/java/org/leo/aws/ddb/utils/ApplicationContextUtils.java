package org.leo.aws.ddb.utils;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

@SuppressWarnings({"unchecked", "SameParameterValue", "unused"})
public class ApplicationContextUtils {
    private static volatile ApplicationContextUtils INSTANCE;
    private final ApplicationContext applicationContext;
    private final Environment environment;

    private ApplicationContextUtils(ApplicationContext applicationContext, Environment environment) {
        this.applicationContext = applicationContext;
        this.environment = environment;
    }

    public static ApplicationContextUtils getInstance() {
        return INSTANCE;
    }

    public <T> T getBean(final String name) {
        return (T) applicationContext.getBean(name);
    }

    public <T> T getBean(final Class<T> type) {
        return applicationContext.getBean(type);
    }

    public <T> T getBean(final Class<T> type, final String qualifierName) {
        return applicationContext.getBean(qualifierName, type);
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public Environment getEnvironment() {
        return environment;
    }

    private static void init(final ApplicationContext applicationContext, final Environment environment) {
        if(INSTANCE == null) {
            synchronized (ApplicationContextUtils.class) {
                if(INSTANCE == null) {
                    INSTANCE = new ApplicationContextUtils(applicationContext, environment);
                }
            }
        }
    }
}
