package org.leo.aws.ddb.exceptions;


import org.leo.aws.ddb.utils.exceptions.Issue;

@SuppressWarnings({"unused", "RedundantSuppression"})
public class OptimisticLockFailureException extends DbException {
    public OptimisticLockFailureException() {
        super(Issue.OPTIMISTIC_LOCK_ERROR.name());
    }

    public OptimisticLockFailureException(final Throwable cause) {
        super(Issue.OPTIMISTIC_LOCK_ERROR.name(), cause);
    }

    public OptimisticLockFailureException(final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(Issue.OPTIMISTIC_LOCK_ERROR.name(), cause, enableSuppression, writableStackTrace);
    }
}
