package org.leo.aws.ddb.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "org.leo.aws")
public class AwsEnvironmentProperties {
    private String region;
    private String awsAccessKey;
    private String awsAccessKeySecret;

    public String getRegion() {
        return region;
    }

    public void setRegion(final String region) {
        this.region = region;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(final String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsAccessKeySecret() {
        return awsAccessKeySecret;
    }

    public void setAwsAccessKeySecret(final String awsAccessKeySecret) {
        this.awsAccessKeySecret = awsAccessKeySecret;
    }
}
