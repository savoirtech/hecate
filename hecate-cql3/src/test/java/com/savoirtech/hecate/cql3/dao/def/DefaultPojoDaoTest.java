package com.savoirtech.hecate.cql3.dao.def;

import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.entities.SimplePojo;
import com.savoirtech.hecate.cql3.test.CassandraTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

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
}