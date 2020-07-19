package org.leo.ddb.repositories;

/**
 * @deprecated Present to maintain backward compatibility. Will be removed soon. Please use {@link NonBlockingBaseRepository}
 * @param <T>
 */
@Deprecated
public interface BaseRepository<T> extends NonBlockingBaseRepository<T> {
}
