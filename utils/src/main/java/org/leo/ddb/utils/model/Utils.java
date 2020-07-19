package org.leo.ddb.utils.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.leo.ddb.utils.exceptions.Issue;
import org.leo.ddb.utils.exceptions.UtilsException;
import rx.functions.Action0;
import rx.functions.Func1;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

@SuppressWarnings({"WeakerAccess"})
public final class Utils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Long MAX_RETRY_INTERVAL_IN_SECONDS_VAL = 30L;
    private static final Thread SHUTDOWN_HOOK = new Thread();
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    private Utils() {
    }


    public static <X> List<X> constructListFromJson(Class<X> paramType, final String json) {
        return constructListFromJson(paramType, json, e -> new UtilsException(Issue.INVALID_JSON, "Invalid Json", e));
    }

    public static <X> List<X> constructListFromJson(Class<X> paramType, final String json, final Func1<Throwable, ? extends RuntimeException> exceptionFunc) {
        try {
            return OBJECT_MAPPER.readValue(json, OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, paramType));
        } catch (IOException e) {
            throw exceptionFunc.call(e);
        }
    }

    public static <T> T constructFromJson(final Class<T> clazz, final String json) {
        return constructFromJson(clazz, json, e -> new UtilsException(Issue.INVALID_JSON, "Invalid Json", e));
    }

    public static <T> T constructFromJson(final Class<T> clazz, final String json, final Func1<Throwable, ? extends RuntimeException> exceptionFunc) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (final IOException e) {
            throw exceptionFunc.call(e);
        }
    }

    public static <T> String constructJson(final T message) {
        return constructJson(message, e -> new UtilsException(Issue.JSON_SERIALIZE_ERROR, "Error while constructing json", e));
    }

    public static <T> String constructJson(final T message, final Func1<Throwable, ? extends RuntimeException> exceptionFunc) {
        try {
            return OBJECT_MAPPER.writeValueAsString(message);
        } catch (final JsonProcessingException e) {
            throw exceptionFunc.call(e);
        }
    }

    public static void executeUsingLock(final Lock lock, final Action0 function) {
        lock.lock();

        try {
            function.call();
        } finally {
            lock.unlock();
        }
    }


    public static <T> T constructObject(final Constructor<T> constructor) {
        try {
            return constructor.newInstance();
        } catch (final Exception e) {
            throw new UtilsException(Issue.INVALID_JSON, e);
        }
    }

    public static <T> T constructObject(final Constructor<T> constructor, final Object... args) {
        try {
            return constructor.newInstance(args);
        } catch (final Exception e) {
            throw new UtilsException(Issue.INVALID_JSON, e);
        }
    }

    public static Object invokeMethod(final String methodName, final Class<?> clazz, final Object obj, final Class<?>[] argTypes, final Object[] args) {
        try {
            return argTypes != null && argTypes.length > 0 ?
                    invokeMethod(clazz.getDeclaredMethod(methodName, argTypes), obj, args) :
                    invokeMethod(clazz.getDeclaredMethod(methodName), obj, args);
        } catch (NoSuchMethodException e) {
            throw new UtilsException(MessageFormat.format("Method [{0}] not found in class [{1}]: {2}", methodName, clazz, e), e);
        }
    }

    public static Object invokeMethod(final Method method, final Object obj, final Object... args) {
        method.setAccessible(true);

        try {
            return args != null && args.length > 0 ? method.invoke(obj, args) : method.invoke(obj);
        } catch (Exception e) {
            throw new UtilsException(MessageFormat.format("Error while invoking method [{0}]: {1}", method.getName(), e), e);
        }
    }

    public static <T> T getFromFromFuture(final Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new UtilsException(Issue.UNKNOWN_ERROR, e);
        } catch (ExecutionException e) {
            final Throwable t = e.getCause();

            throw t instanceof RuntimeException ? (RuntimeException) t : new UtilsException(Issue.UNKNOWN_ERROR, t);
        }
    }

    public static <T> T constructObject(final Class<T> clazz) {
        try {
            final Constructor<T> constructor = clazz.getDeclaredConstructor();

            constructor.setAccessible(true);

            return constructObject(constructor);
        } catch (final Exception e) {
            throw new UtilsException(Issue.UNKNOWN_ERROR, e);
        }
    }

    public static List<Object> convertArrayToList(final Class<?> arrayType, final Object value) {
        final int length = Array.getLength(value);
        final ImmutableList.Builder<Object> builder = ImmutableList.builder();

        for(int i = 0; i < length; i++) {
            builder.add(Array.get(value, i));
        }

        return builder.build();
    }

}
