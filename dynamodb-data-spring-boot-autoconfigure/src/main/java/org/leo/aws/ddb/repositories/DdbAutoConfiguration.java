package org.leo.aws.ddb.repositories;

import org.leo.aws.ddb.autoconfigure.AwsEnvironmentProperties;
import org.leo.aws.ddb.autoconfigure.DynamoDbProperties;
import org.leo.aws.ddb.config.EntityValidationConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.Map;

@SuppressWarnings("rawtypes")
@Configuration
@ConditionalOnClass({DynamoDbRepository.class})
@EnableConfigurationProperties({AwsEnvironmentProperties.class, DynamoDbProperties.class})
@Import(DataMapperConfig.class)
public class DdbAutoConfiguration {
    @Value("${org.leo.aws.ddb.entities.basePackage:org.leo}")
    private String dtoBasePackage;


    @Bean
    @ConditionalOnProperty(prefix = "org.leo.aws", value = {"aws-access-key-secret", "aws-access-key"})
    public AwsCredentialsProvider staticCredentialsProvider(final AwsEnvironmentProperties dynamoDbProperties) {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(dynamoDbProperties.getAwsAccessKey(), dynamoDbProperties.getAwsAccessKeySecret()));
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "org.leo.aws", value = {"region"})
    public DynamoDbAsyncClient amazonDynamoDB(final AwsCredentialsProvider staticCredentialsProvider, final AwsEnvironmentProperties dynamoDbProperties) {

        return DynamoDbAsyncClient.builder().region(Region.of(dynamoDbProperties.getRegion())).credentialsProvider(staticCredentialsProvider).build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "org.leo.aws", value = {"region"})
    public DynamoDbAsyncClient amazonDynamoDBEnv(final AwsEnvironmentProperties dynamoDbProperties) {

        return DynamoDbAsyncClient.builder().region(Region.of(dynamoDbProperties.getRegion())).build();
    }

    @Bean(name = "entityValidationConfigMain")
    @ConditionalOnProperty(prefix = "org.leo.aws.ddb", value = "entity-base-package")
    public EntityValidationConfig entityValidationConfigMain(final DynamoDbProperties dynamoDbProperties) {
        return new EntityValidationConfig(dynamoDbProperties.getEntityBasePackage());
    }

    //TODO For backward compatibility. Remove later
    @Bean(name = "entityValidationConfigTmp")
    @ConditionalOnMissingBean(name = "entityValidationConfigMain")
    public EntityValidationConfig entityValidationConfigTmp() {
        return new EntityValidationConfig(dtoBasePackage);
    }

    @Bean(name = "dataMapperConfigCleanUpMain")
    @ConditionalOnProperty(prefix = "org.leo.aws.ddb", value = "entity-base-package")
    public DataMapperConfigCleanUp dataMapperConfigCleanUpMain(final DynamoDbProperties dynamoDbProperties, final Map<Class, DataMapper> dataMapperMap, final Environment environment) {
        return new DataMapperConfigCleanUp(dynamoDbProperties.getEntityBasePackage(), dataMapperMap, environment);
    }

    @Bean(name = "dataMapperConfigCleanUp")
    @ConditionalOnMissingBean(name = {"dataMapperConfigCleanUpMain", "entityValidationConfigMain"})
    public DataMapperConfigCleanUp dataMapperConfigCleanUpTmp(final Map<Class, DataMapper> dataMapperMap, final Environment environment) {
        return new DataMapperConfigCleanUp(dtoBasePackage, dataMapperMap, environment);
    }
}
