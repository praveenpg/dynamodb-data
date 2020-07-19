package org.leo.ddb.utils.exceptions;

public class UtilsException extends ServiceException {
    public UtilsException(final String errorType) {
        super(errorType);
    }

    public UtilsException(final Issue errorType) {
        super(errorType.name());
    }

    public UtilsException(final String errorType, final String message) {
        super(errorType, message);
    }

    public UtilsException(final Issue errorType, final String message) {
        super(errorType.name(), message);
    }

    public UtilsException(final String errorType, final String message, final Throwable cause) {
        super(errorType, message, cause);
    }

    public UtilsException(final Issue errorType, final String message, final Throwable cause) {
        super(errorType.name(), message, cause);
    }

    public UtilsException(final String errorType, final Throwable cause) {
        super(errorType, cause);
    }

    public UtilsException(final Issue errorType, final Throwable cause) {
        super(errorType.name(), cause);
    }
}
