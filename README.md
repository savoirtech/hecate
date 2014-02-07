hecate
======

Hecate came out of an ASF Licensed effort, originally created by Jeff Genender.

It's initial usage is described here https://oracleus.activeevents.com/2013/connect/sessionDetail.ww?SESSION_ID=3674


This has become a library that we use frequently at http://savoirtech.com as we very rapidly can build
new data models, index, and search those.

It is a library that has undergone several iterations and heavy load testing.
In the simplest DAO solution we hold around 500 req/s against an 8 node VM based cluster.
(We never got any further than that since the surrounding HW failed)


Starting with the "Fixed DAO"

```Java

@Test
    public void testCNE() {
        CassandraKeyspaceConfigurator keyspaceConfigurator = new CassandraKeyspaceConfigurator(getHost(), KEYSPACE, getFailoverPolicy(), getConsistencyLevelPolicy(), getCredentials());
        CNEDao dao = new CNEDaoImpl(CLUSTER, keyspaceConfigurator, String.class, CNEColumn.class, CF_CNE);

        //Save it first
        CNEColumn col = new CNEColumn();

        CNE cneRecord = new CNE();
        cneRecord.setProvincia("MANABI");
        cneRecord.setCanton("BOLIVAR");
        cneRecord.setParroquia("CALCETA");
        cneRecord.setRecinto("");
        cneRecord.setCedula("13155555231");
        cneRecord.setNombre("MIRANDA GANCHOZO CRISTHIAN ALBERTO");
        cneRecord.setFecha("2011-07-24");
        cneRecord.setCertificado("156-0018");
        cneRecord.setSufrago(true);
        cneRecord.setMulta(0.00);
        cneRecord.setPartido("MUCHO");
        cneRecord.setTipo("");

        col.setCne(cneRecord);
        dao.save(cneRecord.getCedula(), col);

        //Did it save?
        CNEColumn getCol = dao.find("131042379231");
        assertNotNull(getCol);
        CNE newCne = getCol.getCne();
        assertNotNull(newCne);

        assertEquals(cneRecord.getProvincia(), newCne.getProvincia());
        assertEquals(cneRecord.getCanton(), newCne.getCanton());
        assertEquals(cneRecord.getCertificado(), newCne.getCertificado());
        assertEquals(cneRecord.getCedula(), newCne.getCedula());
        assertEquals(cneRecord.getFecha(), newCne.getFecha());
        assertEquals(cneRecord.getNombre(), newCne.getNombre());
        assertEquals(cneRecord.getParroquia(), newCne.getParroquia());
        assertEquals(cneRecord.getPartido(), newCne.getPartido());
        assertEquals(cneRecord.getRecinto(), newCne.getRecinto());
        assertEquals(cneRecord.getTipo(), newCne.getTipo());
        assertEquals(cneRecord.getMulta(), newCne.getMulta());
        assertEquals(cneRecord.getSufrago(), newCne.getSufrago());

        //Force a serialization error (this usually means the data is bad or old)
        boolean  errFired = false;
        try {
            getCol = dao.find("12345678");
        } catch (ObjectNotSerializableException e) {
            errFired = true;
        }

        if (!errFired){
            fail("ObjectNotSerializable did not fire when looking up bad data with cedula 12345678!");
        }
    }

```

THe dao class we use here looks like this -

```Java
public class CNEColumn {

    private CNE cne;

    public CNE getCne() {
        return cne;
    }

    public void setCne(CNE cne) {
        this.cne = cne;
    }
}
```

Essentially we relied on small rows and really rapid storage of binary data.

Then we introduced indexing (Composite column Families)

```Java
 @Test
    public void testCTECedulaTipOrigenDao() throws Exception {

        CassandraKeyspaceConfigurator keyspaceConfigurator = new CassandraKeyspaceConfigurator(getHost(), KEYSPACE, getFailoverPolicy(), getConsistencyLevelPolicy(), getCredentials());
        CTECedulaTipOrigenDao dao = new CTECedulaTipoOrigenDaoImpl(CLUSTER, keyspaceConfigurator, String.class,
            CTECedulaTipOrigenCompositeColumn.class, UUID.class, CF_CTE_CEDULA_TIPO_ORIGEN);

        ColumnIterator<String, CTECedulaTipOrigenCompositeColumn, UUID> iterator = dao.find("130893080");
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        Column<CTECedulaTipOrigenCompositeColumn, UUID> col = iterator.next();
        assertEquals("B", col.getName().getTipo());
        assertEquals("CTE", col.getName().getOrigen());
        assertEquals(UUID.fromString("810f8d30-d832-11e1-9af9-e0f84722b90c"), col.getValue());
    }

 ```

 After a few more projects we landed in the zone of having to replace more and more RDBMS solutions
 so we added a few other pojos.


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



