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

And a DAO to utilize the Java Object.

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

Pojo graph indexing
===================

This feature will recursively traverse and persist the dao 
layer adding fields and column families as needed.
The Dao relies on generics and collection declaration.
It is not suggested you use this pojo on larger object graphs
or circular references.

```Java
public class PojoGraphDaoTest extends AbstractCassandraTest {

    @Test
    public void genericCollectionsToCF() {

        DaoPool<PojoObjectGraphDao> daoDaoPool = new DaoPool<>("cmp", keyspaceConfigurator, PojoObjectGraphDao.class);
        ColumnFamilyDao dao = daoDaoPool.getPojoDao(String.class, Top.class, "REDIRECT", null);

        Top top = new Top();
        top.setId("A");
        Child child = new Child();
        child.setId("A");

        top.getChildren().add(child);
        top.getMoreKids().add(child);

        dao.save(top.getId(), top);

        Top newTop = (Top) dao.find("A");

        assertTrue(newTop.getMoreKids().size() == 1);
    }

    @Test
    public void stringTest() {

        DaoPool<PojoObjectGraphDao> daoDaoPool = new DaoPool<>("cmp", keyspaceConfigurator, PojoObjectGraphDao.class);
        ColumnFamilyDao dao = daoDaoPool.getPojoDao(String.class, TopWithString.class, "REDIRECT", null);

        TopWithString top = new TopWithString();
        top.setId("A");
        Child child = new Child();
        child.setId("A");

        top.getChildren().add(child);
        top.getMoreKids().add(child);
        top.getStrings().add("A");

        dao.save(top.getId(), top);

        TopWithString newTop = (TopWithString) dao.find("A");

        assertTrue(newTop.getMoreKids().size() == 1);
        assertTrue(newTop.getStrings().size() == 1);
    }
}
```



