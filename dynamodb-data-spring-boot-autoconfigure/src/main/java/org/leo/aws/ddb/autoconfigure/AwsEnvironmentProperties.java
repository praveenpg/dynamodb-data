package org.leo.aws.ddb.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@ConfigurationProperties(prefix = "org.leo.aws")
public class AwsEnvironmentProperties {
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private String region;
    private String awsAccessKey;
    private String awsAccessKeySecret;
    private int maxConcurrency = 100;
    private int maxPendingConnectionAcquires = 10000;

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

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(final int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public int getMaxPendingConnectionAcquires() {
        return maxPendingConnectionAcquires;
    }

    public void setMaxPendingConnectionAcquires(final int maxPendingConnectionAcquires) {
        this.maxPendingConnectionAcquires = maxPendingConnectionAcquires;
    }
}
