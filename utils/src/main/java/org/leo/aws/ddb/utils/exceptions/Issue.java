package org.leo.aws.ddb.utils.exceptions;

public enum Issue {
    UNKNOWN_ERROR,
    JSON_SERIALIZE_ERROR,
    INVALID_JSON,
    INCORRECT_MODEL_ANNOTATION,
    RECORD_ALREADY_EXISTS,
    OPTIMISTIC_LOCK_ERROR
}
