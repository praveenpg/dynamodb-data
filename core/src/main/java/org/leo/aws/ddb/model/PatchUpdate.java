package org.leo.aws.ddb.model;

@SuppressWarnings("unused")
public interface PatchUpdate {
    Type getOp() ;

    String getPath();

    String getValue();

    enum Type {
        replace, remove
    }
}
