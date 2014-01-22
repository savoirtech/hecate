package com.savoirtech.hecate;

import com.savoirtech.hecate.test.CassandraTestCase;
import com.savoirtech.hecate.util.Person;
import me.prettyprint.cassandra.dao.SimpleCassandraDao;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class CassandraDaoTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void createColumnFamily() {
        createColumnFamily("people");
    }

    @Test
    public void testSave() throws Exception {


        CassandraDao<String, String, Person> dao = newPersonDao();

        Person person = new Person();
        person.setFirstName("Hector");
        person.setLastName("Cassandra");
        dao.save("12345", person);

        SimpleCassandraDao cassandraDao = new SimpleCassandraDao();
        cassandraDao.setColumnFamilyName("people");
        cassandraDao.setKeyspace(keyspace);
        assertEquals("Hector", getColumnValueAsString("people", "12345", "firstName"));
        assertEquals("Cassandra", getColumnValueAsString("people", "12345", "lastName"));
    }

    protected CassandraDao<String, String, Person> newPersonDao() {
        return new CassandraDao<String, String, Person>(keyspace, "people", String.class, String.class, Person.class, new PersonMapper());
    }

    @Test
    public void testFind() {
        CassandraDao<String, String, Person> dao = newPersonDao();
        Person person = new Person();
        person.setFirstName("Hector");
        person.setLastName("Cassandra");
        dao.save("12345", person);

        Person found = dao.find("12345");
        assertNotNull(found);
        assertEquals("Hector", found.getFirstName());
        assertEquals("Cassandra", found.getLastName());
    }

    @Test
    public void testDelete() {
        CassandraDao<String, String, Person> dao = newPersonDao();
        Person person = new Person();
        person.setFirstName("Hector");
        person.setLastName("Cassandra");
        dao.save("12345", person);

        assertNotNull(dao.find("12345"));
        dao.delete("12345");
        assertNull(dao.find("12345"));
    }


    private static class PersonMapper implements ColumnMapper<String, Person> {
        @Override
        public Set<HColumn<String, ?>> toColumns(Person object) {
            Set<HColumn<String, ?>> columns = new HashSet<HColumn<String, ?>>();
            columns.add(HFactory.createColumn("firstName", object.getFirstName()));
            columns.add(HFactory.createColumn("lastName", object.getLastName()));
            return columns;
        }

        @Override
        public void fromColumns(Person object, List<HColumn<String, byte[]>> hColumns) {
            final StringSerializer serializer = StringSerializer.get();
            for (HColumn<String, byte[]> column : hColumns) {
                if ("firstName".equals(column.getName())) {
                    object.setFirstName(serializer.fromBytes(column.getValue()));
                }
                if ("lastName".equals(column.getName())) {
                    object.setLastName(serializer.fromBytes(column.getValue()));
                }
            }
        }
    }
}
