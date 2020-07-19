package org.leo.ddb.exceptions;


import org.leo.ddb.utils.exceptions.Issue;

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
