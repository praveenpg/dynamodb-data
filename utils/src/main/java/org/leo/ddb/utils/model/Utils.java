package org.leo.ddb.utils.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.leo.ddb.utils.exceptions.Issue;
import org.leo.ddb.utils.exceptions.UtilsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.StringUtils;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

@SuppressWarnings({"unused", "WeakerAccess"})
public final class Utils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int VISIBILITY_TIMEOUT = 60;
    private static final Long MAX_RETRY_INTERVAL_IN_SECONDS_VAL = 30L;
    private static final Long MAX_RETRY_INTERVAL_IN_SECONDS = TimeUnit.MINUTES.toSeconds(MAX_RETRY_INTERVAL_IN_SECONDS_VAL);
    private static final int MAXIMUM_NO_OF_RETRIES = 5;
    private static final int VISIBILITY_TIMEOUT_MULTIPLICATION_FACTOR = 2;
    private static final Thread SHUTDOWN_HOOK = new Thread();
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

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

    public static <T, R> R convert(final T obj1, final Class<R> type) {
        return convert(obj1, type, OBJECT_MAPPER);
    }

    public static <T, R> List<R> convertToList(final T obj1, final Class<R> type) {
        return convertToList(obj1, type, OBJECT_MAPPER);
    }

    public static <T, R> R convert(final T obj1, final Class<R> type, final ObjectMapper mapper) {
        return mapper.convertValue(obj1, type);
    }

    public static <T, R> List<R> convertToList(final T obj1, final Class<R> type, final ObjectMapper mapper) {
        return OBJECT_MAPPER.convertValue(obj1, OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, type));
    }

    public static int calculateVisibilityTimeout(final int retryNumber) {
        final int visibilityTimeoutInSeconds;

        if(retryNumber <= MAXIMUM_NO_OF_RETRIES) {
            visibilityTimeoutInSeconds = VISIBILITY_TIMEOUT * (int)Math.pow(VISIBILITY_TIMEOUT_MULTIPLICATION_FACTOR, retryNumber > 0 ?
                    (retryNumber - 1) : retryNumber);
        } else {
            visibilityTimeoutInSeconds = VISIBILITY_TIMEOUT * (int)Math.pow(VISIBILITY_TIMEOUT_MULTIPLICATION_FACTOR, MAXIMUM_NO_OF_RETRIES);
        }

        return (visibilityTimeoutInSeconds > MAX_RETRY_INTERVAL_IN_SECONDS) ? MAX_RETRY_INTERVAL_IN_SECONDS.intValue()
                : visibilityTimeoutInSeconds;
    }

    public static <T> T executeUsingLock(final Lock lock, final Supplier<T> function) {
        lock.lock();

        try {
            return function.get();
        } finally {
            lock.unlock();
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

    public static void executeUsingSemaphore(final Semaphore semaphore, final long timeout, final TimeUnit timeUnit, final Action0 function) throws InterruptedException {
        final boolean lockAcquired = semaphore.tryAcquire(timeout, timeUnit);

        try {
            if(lockAcquired) {
                function.call();
            }
        } finally {
            if(lockAcquired) {
                semaphore.release();
            }
        }
    }

    public static void executeUsingSemaphore(final Semaphore semaphore, final Action0 function) {
        executeUsingSemaphore(semaphore, function, e -> new UtilsException(Issue.UNKNOWN_ERROR, "Error when executing method: ", e));
    }

    public static void executeUsingSemaphore(final Semaphore semaphore, final Action0 function, final Func1<Throwable, ? extends RuntimeException> exceptionFunc) {

        try {
            semaphore.acquire();
            function.call();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw exceptionFunc.call(e);
        } finally {
            semaphore.release();
        }
    }

    public static <T> T executeUsingSemaphore(final Semaphore semaphore, final long timeout, final TimeUnit timeUnit, final Func0<T> function) throws InterruptedException {
        return executeUsingSemaphore(semaphore, timeout, timeUnit, function, () -> new UtilsException(Issue.UNKNOWN_ERROR, "Error when executing method: "));
    }

    public static <T> T executeUsingSemaphore(final Semaphore semaphore, final long timeout, final TimeUnit timeUnit, final Func0<T> function, final Func0<? extends RuntimeException> exceptionFunc) throws InterruptedException {
        final boolean lockAcquired = semaphore.tryAcquire(timeout, timeUnit);

        try {
            if(lockAcquired) {
                return function.call();
            } else {
                throw exceptionFunc.call();
            }
        } finally {
            if(lockAcquired) {
                semaphore.release();
            }
        }
    }

    public static <T> T executeUsingSemaphore(final Semaphore semaphore, final Func0<T> function) {
        return executeUsingSemaphore(semaphore, function, e -> new UtilsException(Issue.UNKNOWN_ERROR, "Error when executing function: " + e, e));
    }

    public static <T> T executeUsingSemaphore(final Semaphore semaphore, final Func0<T> function, final Func1<Throwable, ? extends RuntimeException> exceptionFunc) {
        try {
            semaphore.acquire();

            return function.call();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw exceptionFunc.call(e);
        } finally {
            semaphore.release();
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

    public static <T> Constructor<T> getConstructor(final Class<T> clazz, final Class<?>... params) {
        return getConstructor(clazz, e -> {}, params);
    }

    public static <T> Constructor<T> getConstructor(final Class<T> clazz, final Action1<? super Exception> errorHandleFunc, final Class<?>... params) {
        try {
            final Constructor<T> constructor = clazz.getDeclaredConstructor(params);

            constructor.setAccessible(true);

            return constructor;
        } catch (final Exception e) {
            errorHandleFunc.call(e);
            throw new UtilsException(Issue.UNKNOWN_ERROR, e);
        }
    }

    public static <T> Method getMethod(final Class<T> clazz, final String methodName, final Class<?>... params) {
        try {
            final Method method = clazz.getDeclaredMethod(methodName, params);

            method.setAccessible(true);

            return method;
        } catch (final Exception e) {
            throw new UtilsException(Issue.UNKNOWN_ERROR, e);
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

    public static void setStaticFieldValue(final String fieldName, final Class<?> clazz) {
        try {
            final Field field = clazz.getDeclaredField("messageEnumType");

            field.setAccessible(true);
            field.set(null, clazz);
        } catch (final Exception e) {
            throw new UtilsException(Issue.UNKNOWN_ERROR, e);
        }
    }

    public static <R> R executeWithTransactionId(final Func0<R> func, final String transactionId) {
        final boolean transactionIDEmpty = StringUtils.isEmpty(transactionId);

        try {
            if(!transactionIDEmpty) {
                MDC.put(UtilsConstants.SFLY_TRANSACTION_ID.getValue(), transactionId);
            }

            return func.call();
        } finally {
            if(!transactionIDEmpty) {
                MDC.remove(UtilsConstants.SFLY_TRANSACTION_ID.getValue());
            }
        }
    }

    public static void executeWithTransactionId(final Action0 func, final String transactionId) {
        final boolean transactionIDEmpty = StringUtils.isEmpty(transactionId);

        try {
            if(!transactionIDEmpty) {
                MDC.put(UtilsConstants.SFLY_TRANSACTION_ID.getValue(), transactionId);
            }

            func.call();
        } finally {
            if(!transactionIDEmpty) {
                MDC.remove(UtilsConstants.SFLY_TRANSACTION_ID.getValue());
            }
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

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isJVMShuttingDown() {
        return executeUsingLock(LOCK.writeLock(), () -> {
            try {
                //Runtime will not allow adding a shutdown hook if it is in the process of shutting down
                Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
                Runtime.getRuntime().removeShutdownHook(SHUTDOWN_HOOK);
            } catch (final IllegalStateException e) {
                //This is thrown only if the JVM is in the process of shutting down
                return true;
            }

            return false;
        });
    }

    public static <T> T handleInterruptedException(final InterruptedException ex, Func0<T> func) {
        if(!isJVMShuttingDown()) {
            LOGGER.error("Interrupted Exception: " + ex, ex);
            Thread.currentThread().interrupt();
            throw new UtilsException(Issue.UNKNOWN_ERROR, ex);
        } else {
            LOGGER.warn("JVM is shutting down....");

            return func.call();
        }
    }

    public static void handleInterruptedException(final InterruptedException ex, Action0 func) {
        if(!isJVMShuttingDown()) {
            LOGGER.error("Interrupted Exception: " + ex, ex);
            Thread.currentThread().interrupt();
            throw new UtilsException(Issue.UNKNOWN_ERROR, ex);
        } else {
            LOGGER.warn("JVM is shutting down....");

            func.call();
        }
    }
}
