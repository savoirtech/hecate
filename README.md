hecate
======


Mapping a Pojo Class -

    A Java object can be utilized, it will handle Collections and Dictionaries.
    For complex object graphs the fields will utilize Serialization.

```Java
public class Segment {

    private String expression;
    private String id;
    private String name;
    private Map<String, String> properties = new HashMap<>();

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getProperty(String property) {
        return properties.get(property);
    }

    public void setProperty(String key, String value) {
        properties.put(key, value);
    }
}
```

And a DAO to utilize the Java

```Java
public class SegmentPersistenceTest {



    @Before
    public void start() throws ConfigurationException, IOException, TTransportException {
        Map<String, String> credentials = new HashMap<String, String>();

        credentials.put(IAuthenticator.USERNAME_KEY, "admin");
        credentials.put(IAuthenticator.PASSWORD_KEY, "secret");

        persistence.setMasterHost("localhost:9175");

        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", credentials);
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Test
    public void testStartupAndSystem() throws InvalidPersistenceData {

        Segment segment = new Segment();
        segment.setId("1");

        ColumnFamilyDao dao = getPojoDao(clusterName, keySpaceConfigs.get(masterKeyspace), String.class, Segment.class, SEGMENT_CF, null);
        dao.save(segment.getId(), segment);

        Segment b = return (Segment) dao.find("1");

        assertTrue(b != null);
    }
}

```


