package org.leo.ddb.utils.model;

public enum UtilsConstants {
    LOGGED_IN_USER,
    SFLY_TRANSACTION_ID("SFLY-TransactionId");

    private final String value;

    @SuppressWarnings("unused")
    UtilsConstants(final String value) {
        this.value = value;
    }

    UtilsConstants() {
        this.value = name();
    }

    public String getValue() {
        return value;
    }
}
