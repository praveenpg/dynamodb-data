package org.leo.aws.ddb.model;


import org.leo.aws.ddb.annotations.MappedBy;
import org.leo.aws.ddb.annotations.VersionAttribute;

public class VersionedEntity {
    @VersionAttribute
    @MappedBy("version")
    private Integer version;

    public Integer getVersion() {
        return version;
    }

    public void setVersion(final Integer version) {
        this.version = version;
    }
}
