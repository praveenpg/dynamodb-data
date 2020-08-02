# dynamodb-data
An ORM (almost) for dynamoDb

## General Information
- Project Name: DynamoDB Data
- GIT URL: <https://github.com/praveenpg/dynamodb-data>
## Requirements
- Java 8
- Maven

## Amazon Services Covered
- DynamoDB (https://aws.amazon.com/dynamodb/)

## Getting Started
```xml
        <dependency>
            <groupId>org.leo.dynamodb</groupId>
            <artifactId>starter</artifactId>
            <version>1.0.0-20200802.053730-1</version>
        </dependency>
```
Add the following to dependencyManagement
```xml
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>software.amazon.awssdk</groupId>
                <artifactId>bom</artifactId>
                <version>2.13.8</version>
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
- Create an entity class with the following annotation
```java
@DDBTable(name = "ddb-demo-user-info")
@Data //Lombok annotation
@AllArgsConstructor //Lombok annotation
public class UserInfo {
    @PK(type = PK.Type.HASH_KEY)
    @GlobalSecondaryIndex(name = "division-emailAddress-index", type = PK.Type.RANGE_KEY, projectionType = GlobalSecondaryIndex.ProjectionType.KEYS_ONLY)
    private String emailAddress;
    @PK(type = PK.Type.RANGE_KEY)
    @GlobalSecondaryIndex(name = "division-emailAddress-index", type = PK.Type.HASH_KEY, projectionType = GlobalSecondaryIndex.ProjectionType.KEYS_ONLY)
    private String division;
    private String firstName;
    private String lastName;
    private String middleName;
    private String password;
}
```
- Add a repository class. The repository class needs to implement the BaseRepository interface.
- Querying by Hash Key and Range Key
- Non Blocking DAO (Preferred)
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
- Blocking DAO
```java
public class UserInfoRepository implements BlockingBaseRepository<UserInfo> {
    public List<UserInfo> getByEmailAddress(String emailAddress) {
        return findByHashKey("emailAddress", emailAddress);
    }
    public List<UserInfo> getByDivision(final String division) {
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
    
    public Mono<UserInfo> findByDivision(final String division) {
        return userInfoRepository.findByGlobalSecondaryIndex("division-emailAddress-index", division);
    }
}
```