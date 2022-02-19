# dynamodb-data
An Object model (almost) for dynamoDb

## General Information
- Project Name: DynamoDB Data\
  This project attempts to develop an Object model for AWS DynamoDB database based systems. Entities are developed as POJOs with annotations as shown in the examples below.
- GIT URL: <https://github.com/praveenpg/dynamodb-data>
## Requirements
- Java 11
- Maven

## Amazon Services Covered
- DynamoDB (https://aws.amazon.com/dynamodb/)

## Getting Started
#### Maven Dependency

```xml
        <dependency>
            <groupId>net.leodb.dynamodb</groupId>
            <artifactId>starter</artifactId>
            <version>1.0.1</version>
        </dependency>
```
Add the following to dependencyManagement
```xml
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>software.amazon.awssdk</groupId>
                <artifactId>bom</artifactId>
                <version>2.17.132</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
```

- Add the following config in either application.yaml or application.properties
```yaml
org:
  leo:
    aws:
      region: us-east-1
      ddb:
        entity-base-package: org.leo.aws.ddb.ddbdemo.entities
        repository-base-package: org.leo.aws.ddb.ddbdemo.dao
      aws-access-key: <access_key>
      aws-access-key-secret: <secret>
```
- Create an entity class with the following annotation. All entity classes should have an empty constructor. It can be private.
```java
@DDBTable(name = "ddb-demo-user-info")
@Data //Lombok annotation (Optional)
@AllArgsConstructor //Lombok annotation (Optional)
public class UserInfo {
    @HashKey
    @SecondaryIndex(name = "division-emailAddress-index", type = KeyType.RANGE_KEY, projectionType = ProjectionType.KEYS_ONLY)
    private String emailAddress;
    @RangeKey
    @SecondaryIndex(name = "division-emailAddress-index", type = KeyType.HASH_KEY, projectionType = ProjectionType.KEYS_ONLY)
    private String division;
    private String firstName;
    private String lastName;
    private String middleName;
    private String password;
}
```
- Add a repository class. The repository class needs to implement the BaseRepository interface.
- Querying by Hash Key and Range Key
```java
@DdbRepository(entityClass = UserInfo.class)
public class UserInfoRepository implements NonBlockingBaseRepository<UserInfo> {
    public Flux<UserInfo> getByEmailAddress(String emailAddress) {
        return findByHashKey("emailAddress", emailAddress);
    }
    public Flux<UserInfo> getByDivision(final String division) {
        return findByGlobalSecondaryIndex("division-emailAddress-index", division);
    }
}

```
- Querying by Hash key and range key
```java
class UserService {
@Autowired
private UserInfoRepository userInfoRepository;

    public Mono<UserInfo> findByEmailAddressAndDivision(final String emailAddress, final String division) {
        return userInfoRepository.findByPrimaryKey(PrimaryKey.builder()
                                                .hashKeyName(userInfoRepository.getHashKeyName())
                                                .hashKeyValue(emailAddress)
                                                .rangeKeyName(userInfoRepository.getRangeKeyName())
                                                .rangeKeyValue(division)
                                                .build());
    }
}
```

- Querying by index
```java
class UserService {
    @Autowired
    private UserInfoRepository userInfoRepository;
    
    public Flux<UserInfo> findByDivision(final String division) {
        return userInfoRepository.findByGlobalSecondaryIndex("division-emailAddress-index", division);
    }
}
```