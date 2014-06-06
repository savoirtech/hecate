package com.savoirtech.hecate.cql3.dao.def;

import com.google.common.collect.Sets;
import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.entities.SimplePojo;
import com.savoirtech.hecate.cql3.test.CassandraTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DefaultPojoDaoTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testSave() throws Exception {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("name");
        dao.save(pojo);

        final SimplePojo found = dao.findByKey(pojo.getId());
        assertNotNull(found);
        assertEquals("name", found.getName());
        assertEquals(pojo.getId(), found.getId());
    }


    @Test
    public void testDelete() throws Exception {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("name");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.delete(pojo.getId());
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testFindByKeys() {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo1 = new SimplePojo();
        pojo1.setName("name1");
        dao.save(pojo1);

        final SimplePojo pojo2 = new SimplePojo();
        pojo2.setName("name2");
        dao.save(pojo2);

        List<SimplePojo> results = dao.findByKeys(Arrays.asList(pojo1.getId(), pojo2.getId()));
        assertEquals(2, results.size());

        assertTrue(results.contains(pojo1));
        assertTrue(results.contains(pojo2));
    }

    @Test
    public void testWithArrayField() {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setInts(new int[]{1, 2, 3});
        dao.save(pojo);
        final SimplePojo found = dao.findByKey(pojo.getId());
        assertArrayEquals(pojo.getInts(), found.getInts());
    }

    @Test
    public void testWithListField() {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setListOfStrings(Arrays.asList("one", "two", "three"));
        dao.save(pojo);
        final SimplePojo found = dao.findByKey(pojo.getId());
        assertNotNull(found.getListOfStrings());
        assertEquals(3, found.getListOfStrings().size());
        assertEquals("one", found.getListOfStrings().get(0));
        assertEquals("two", found.getListOfStrings().get(1));
        assertEquals("three", found.getListOfStrings().get(2));

    }

    @Test
    public void testWithSetField() {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setSetOfStrings(Sets.newHashSet("one", "two", "three"));
        dao.save(pojo);
        final SimplePojo found = dao.findByKey(pojo.getId());
        assertNotNull(found.getSetOfStrings());
        assertEquals(3, found.getSetOfStrings().size());
        assertTrue(found.getSetOfStrings().contains("one"));
        assertTrue(found.getSetOfStrings().contains("two"));
        assertTrue(found.getSetOfStrings().contains("three"));
    }

    @Test
    public void testWithMapField() {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(connect());
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        Map<Integer, String> mapOfStrings = new HashMap<>();
        mapOfStrings.put(1, "one");
        mapOfStrings.put(2, "two");
        mapOfStrings.put(3, "three");
        pojo.setMapOfStrings(mapOfStrings);
        dao.save(pojo);
        final SimplePojo found = dao.findByKey(pojo.getId());
        assertNotNull(found.getMapOfStrings());
        assertEquals(3, found.getMapOfStrings().size());
        assertEquals("one", found.getMapOfStrings().get(1));
        assertEquals("two", found.getMapOfStrings().get(2));
        assertEquals("three", found.getMapOfStrings().get(3));
    }
}