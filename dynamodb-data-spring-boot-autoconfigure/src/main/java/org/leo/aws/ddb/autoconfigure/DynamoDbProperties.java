package org.leo.aws.ddb.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "org.leo.aws.ddb")
public class DynamoDbProperties {
    private String entityBasePackage;
    private String repositoryBasePackage;

    public String getEntityBasePackage() {
        return entityBasePackage;
    }

    public void setEntityBasePackage(final String entityBasePackage) {
        this.entityBasePackage = entityBasePackage;
    }

    public String getRepositoryBasePackage() {
        return repositoryBasePackage;
    }

    public void setRepositoryBasePackage(final String repositoryBasePackage) {
        this.repositoryBasePackage = repositoryBasePackage;
    }
}
