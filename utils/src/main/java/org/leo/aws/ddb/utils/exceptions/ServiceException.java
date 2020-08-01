package org.leo.aws.ddb.utils.exceptions;

public class ServiceException extends RuntimeException {
    private final String errorType;

    public ServiceException(final String errorType) {
        this(errorType, errorType);
    }

    public ServiceException(final String errorType, final String message) {
        super(message);
        this.errorType = errorType;
    }

    public ServiceException(final String errorType, final String message, final Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
    }

    public ServiceException(final String errorType, final Throwable cause) {
        this(errorType, errorType, cause);
    }

    public String getErrorType() {
        return errorType;
    }
}
