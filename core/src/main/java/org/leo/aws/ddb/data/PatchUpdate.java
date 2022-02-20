package org.leo.aws.ddb.data;

@SuppressWarnings("unused")
public interface PatchUpdate {
    Type getOp() ;

    String getPath();

    String getValue();

    enum Type {
        replace, remove
    }
}
