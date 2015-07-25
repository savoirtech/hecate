/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.dao.def;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.collect.Sets;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.core.statement.StatementOptionsBuilder;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.*;
import com.savoirtech.hecate.pojo.entities.time.*;
import com.savoirtech.hecate.pojo.persistence.PojoQuery;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import org.junit.Test;

import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

public class DefaultPojoDaoTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testCascade() {
        final PojoDao<String, CascadedPojo> dao = getFactory().createPojoDao(CascadedPojo.class);
        final PojoDao<String, NestedPojo> nestedDao = getFactory().createPojoDao(NestedPojo.class);
        CascadedPojo pojo = new CascadedPojo();
        pojo.setDeleteOnly(new NestedPojo());
        pojo.setSaveOnly(new NestedPojo());
        pojo.setNoCascade(new NestedPojo());
        dao.save(pojo);

        assertNotNull(nestedDao.findById(pojo.getSaveOnly().getId()));
        assertNull(nestedDao.findById(pojo.getDeleteOnly().getId()));
        assertNull(nestedDao.findById(pojo.getNoCascade().getId()));

        pojo.setDeleteOnly(pojo.getSaveOnly());
        dao.save(pojo);

        dao.delete(pojo.getId());

        assertNull(nestedDao.findById(pojo.getSaveOnly().getId()));
        assertNull(nestedDao.findById(pojo.getDeleteOnly().getId()));
        assertNull(nestedDao.findById(pojo.getNoCascade().getId()));
    }

    @Test
    public void testDelete() throws Exception {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("name");
        dao.save(pojo);
        assertNotNull(dao.findById(pojo.getId()));
        dao.delete(pojo.getId());
        assertNull(dao.findById(pojo.getId()));
    }

    @Test
    public void testDeleteWithCompositeKey() {
        DefaultPojoDaoFactory factory = getFactory();
        PojoDao<CompositeKey, CompositeKeyPojo> dao = factory.createPojoDao(CompositeKeyPojo.class);
        final CompositeKeyPojo pojo = new CompositeKeyPojo();
        CompositeKey key = new CompositeKey();
        key.setPart1("a");
        key.setPart2("b");
        key.setCluster1("c");
        pojo.setKey(key);
        pojo.setData("Now is the time for all good men...");
        dao.save(pojo);
        dao.delete(pojo.getKey());
    }

    @Test
    public void testDeleteWithNestedPojos() throws Exception {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final PojoDao<String, NestedPojo> nestedPojoDao = factory.createPojoDao(NestedPojo.class);
        final SimplePojo pojo = new SimplePojo();
        final NestedPojo nestedPojo = new NestedPojo();
        pojo.setNestedPojo(nestedPojo);
        pojo.setPojoArray(new NestedPojo[]{nestedPojo});
        pojo.setPojoList(Collections.singletonList(nestedPojo));
        Map<String, NestedPojo> pojoMap = new HashMap<>();
        pojoMap.put("one", nestedPojo);
        pojo.setPojoMap(pojoMap);
        pojo.setPojoSet(Collections.singleton(nestedPojo));
        pojo.setName("name");
        dao.save(pojo);

        assertNotNull(dao.findById(pojo.getId()));
        assertNotNull(nestedPojoDao.findById(nestedPojo.getId()));

        dao.delete(pojo.getId());
        assertNull(dao.findById(pojo.getId()));
        assertNull(nestedPojoDao.findById(nestedPojo.getId()));
    }

    @Test
    public void testFindByIdWithOptions() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo expected = new SimplePojo();
        dao.save(expected);

        SimplePojo actual = dao.findById(expected.getId(), StatementOptionsBuilder.consistencyLevel(ConsistencyLevel.ONE).build());
        assertEquals(expected, actual);
    }

    @Test
    public void testFindByIdsWithSet() throws Exception {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("name");
        dao.save(pojo);
        assertEquals(1, dao.findByIds(Sets.newHashSet(pojo.getId())).list().size());
    }

    @Test
    public void testFindByIndexedField() {
        DefaultPojoDaoFactory factory = getFactory();

        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("Duke");
        dao.save(pojo);

        final PojoQuery<SimplePojo> query = dao.find().eq("name").build();
        SimplePojo found = query.execute("Duke").one();
        assertNotNull(found);
    }

    @Test
    public void testFindByKeys() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo1 = new SimplePojo();
        pojo1.setName("name1");
        dao.save(pojo1);

        final SimplePojo pojo2 = new SimplePojo();
        pojo2.setName("name2");
        dao.save(pojo2);

        List<SimplePojo> results = dao.findByIds(Arrays.asList(pojo1.getId(), pojo2.getId())).list();
        assertEquals(2, results.size());

        assertTrue(results.contains(pojo1));
        assertTrue(results.contains(pojo2));
    }

    @Test
    public void testInsertWithCompositeKey() {
        DefaultPojoDaoFactory factory = getFactory();
        PojoDao<CompositeKey, CompositeKeyPojo> dao = factory.createPojoDao(CompositeKeyPojo.class);
        final CompositeKeyPojo pojo = new CompositeKeyPojo();
        CompositeKey key = new CompositeKey();
        key.setPart1("a");
        key.setPart2("b");
        key.setCluster1("c");
        pojo.setKey(key);
        pojo.setData("Now is the time for all good men...");
        dao.save(pojo);

        CompositeKeyPojo found = dao.find().eq("key.part1").eq("key.part2").eq("key.cluster1").build().execute("a", "b", "c").one();
        assertNotNull(found);
    }

    @Test
    public void testListWithMultiple() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo1 = new SimplePojo();
        pojo1.setName("pojo1");
        dao.save(pojo1);

        final SimplePojo pojo2 = new SimplePojo();
        pojo2.setName("pojo2");
        dao.save(pojo2);

        MappedQueryResult<SimplePojo> result = dao.find().build().execute();


        final List<SimplePojo> pojos = result.list();
        assertNotSame(pojos.get(0), pojos.get(1));
        assertNotEquals(pojos.get(0), pojos.get(1));
    }

    @Test
    public void testPersistenceWithNoConstructor() {
        final PojoDao<String, NoConstructorPerson> dao = getFactory().createPojoDao(NoConstructorPerson.class);
        final NoConstructorPerson expected = new NoConstructorPerson("1", "Foo", "Bar");
        dao.save(expected);
        final NoConstructorPerson actual = dao.findById("1");
        assertEquals(expected.getFirstName(), actual.getFirstName());
        assertEquals(expected.getLastName(), actual.getLastName());
        assertEquals(expected.getSsn(), actual.getSsn());
    }

    @Test
    public void testQueryWithMixedParameters() {
        CompositeKeyPojo pojo = new CompositeKeyPojo();
        CompositeKey key = new CompositeKey();
        key.setPart1("part1Value");
        key.setPart2("part2Value");
        key.setCluster1("cluster1Value");
        pojo.setKey(key);
        pojo.setData("FooBarBaz");
        final PojoDao<CompositeKey, CompositeKeyPojo> dao = getFactory().createPojoDao(CompositeKeyPojo.class);
        dao.save(pojo);

        PojoQuery<CompositeKeyPojo> query = dao.find().eq("key.part1", "part1Value").eq("key.part2").eq("key.cluster1", "cluster1Value").build();
        MappedQueryResult<CompositeKeyPojo> result = query.execute("part2Value");
        assertEquals(1, result.list().size());
    }

    @Test
    public void testQueryWithMultipleClusteringColumns() {
        ClusteredKeyPojo pojo = new ClusteredKeyPojo();
        ClusteredKey key = new ClusteredKey();
        key.setPartitionKey("partitionKeyValue");
        key.setCluster1("cluster1Value");
        key.setCluster2("cluster2Value");

        pojo.setKey(key);
        pojo.setData("FooBarBaz");
        final PojoDao<ClusteredKey, ClusteredKeyPojo> dao = getFactory().createPojoDao(ClusteredKeyPojo.class);
        dao.save(pojo);

        PojoQuery<ClusteredKeyPojo> query = dao.find().eq("key.partitionKey").eq("key.cluster1").build();
        ClusteredKeyPojo found = query.execute("partitionKeyValue", "cluster1Value").one();
        assertNotNull(found);

        found = dao.find().eq("key.partitionKey").build().execute("partitionKeyValue").one();
        assertNotNull(found);
    }

    @Test
    public void testQueryWithOnlyInjectedParameters() {
        CompositeKeyPojo pojo = new CompositeKeyPojo();
        CompositeKey key = new CompositeKey();
        key.setPart1("part1Value");
        key.setPart2("part2Value");
        key.setCluster1("cluster1Value");
        pojo.setKey(key);
        pojo.setData("FooBarBaz");
        final PojoDao<CompositeKey, CompositeKeyPojo> dao = getFactory().createPojoDao(CompositeKeyPojo.class);
        dao.save(pojo);

        PojoQuery<CompositeKeyPojo> query = dao.find().eq("key.part1", "part1Value").eq("key.part2", "part2Value").build();
        MappedQueryResult<CompositeKeyPojo> result = query.execute();
        assertEquals(1, result.list().size());
    }

    @Test
    public void testSave() throws Exception {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("name");
        dao.save(pojo);

        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found);
        assertEquals("name", found.getName());
        assertEquals(pojo.getId(), found.getId());
    }

    @Test
    public void testSaveWithCustomTableName() throws Exception {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class, "BOB");
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("name");
        dao.save(pojo);

        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found);
        assertEquals("name", found.getName());
        assertEquals(pojo.getId(), found.getId());
    }

    @Test
    public void testSaveWithTtl() throws Exception {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("name");
        dao.save(pojo, 90600);

        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found);
        assertEquals("name", found.getName());
        assertEquals(pojo.getId(), found.getId());

        ResultSet resultSet = getSession().execute("SELECT TTL (name) from simpletons");
        for (Row row : resultSet) {
            assertTrue(row.getInt(0) <= 90600);
        }
    }

    @Test
    public void testSelectAll() {
        DefaultPojoDaoFactory factory = getFactory();

        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("Duke");
        dao.save(pojo);

        final PojoQuery<SimplePojo> query = dao.find().build();
        SimplePojo found = query.execute().one();
        assertNotNull(found);
    }

    @Test(expected = InvalidQueryException.class)
    public void testWhenNotLoggedIntoKeyspace() {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(getCluster().connect());
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("Nope");
        dao.save(pojo);
    }

    @Test
    public void testWithArrayField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setInts(new int[]{1, 2, 3});
        dao.save(pojo);
        final SimplePojo found = dao.findById(pojo.getId());
        assertArrayEquals(pojo.getInts(), found.getInts());
    }

    @Test
    public void testWithEmbeddedObjects() {
        PojoDao<String, Person> dao = getFactory().createPojoDao(Person.class);

        Address address = new Address();
        address.setStreet1("123 Main St.");
        address.setStreet2("Apt. B");
        address.setCity("Winchestertonfieldville");
        address.setState("IA");
        address.setZip("12345");
        Person person = new Person();
        person.setHomeAddress(address);
        person.setFirstName("Doctor");
        person.setLastName("Pepper");
        dao.save(person);

        Person found = dao.findById(person.getSsn());
        assertEquals(address.getStreet1(), found.getHomeAddress().getStreet1());
    }

    @Test
    public void testWithEnumField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setNums(SimplePojo.Nums.Three);
        dao.save(pojo);
        final SimplePojo found = dao.findById(pojo.getId());
        assertEquals(SimplePojo.Nums.Three, found.getNums());
    }

    @Test
    public void testWithInjectedParameters() {
        DefaultPojoDaoFactory factory = getFactory();

        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("Duke");
        dao.save(pojo);

        final PojoQuery<SimplePojo> query = dao.find().eq("id", pojo.getId()).build();
        SimplePojo found = query.execute().one();
        assertNotNull(found);
    }

    @Test
    public void testWithListField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setListOfStrings(Arrays.asList("one", "two", "three"));
        dao.save(pojo);
        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found.getListOfStrings());
        assertEquals(3, found.getListOfStrings().size());
        assertEquals("one", found.getListOfStrings().get(0));
        assertEquals("two", found.getListOfStrings().get(1));
        assertEquals("three", found.getListOfStrings().get(2));
    }

    @Test
    public void testWithMapField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        Map<Integer, String> mapOfStrings = new HashMap<>();
        mapOfStrings.put(1, "one");
        mapOfStrings.put(2, "two");
        mapOfStrings.put(3, "three");
        pojo.setMapOfStrings(mapOfStrings);
        dao.save(pojo);
        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found.getMapOfStrings());
        assertEquals(3, found.getMapOfStrings().size());
        assertEquals("one", found.getMapOfStrings().get(1));
        assertEquals("two", found.getMapOfStrings().get(2));
        assertEquals("three", found.getMapOfStrings().get(3));
    }

    @Test
    public void testWithMixedParameters() {
        DefaultPojoDaoFactory factory = getFactory();

        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("Nums");
        pojo.setNums(SimplePojo.Nums.Three);
        dao.save(pojo);
        final PojoQuery<SimplePojo> query = dao.find().eq("nums").eq("id", pojo.getId()).build();
        SimplePojo found = query.execute(SimplePojo.Nums.Three).one();
        assertNotNull(found);
    }

    @Test
    public void testWithMultiFacetKeyPojo() {
        DefaultPojoDaoFactory factory = getFactory();
        PojoDao<Object, MultiFacetKeyPojo> dao = factory.createPojoDao(MultiFacetKeyPojo.class);
        final MultiFacetKeyPojo pojo = new MultiFacetKeyPojo();
        pojo.setPartKey("part1");
        pojo.setClust1("clust1");
        pojo.setClust2("clust2");
        dao.save(pojo);
        MultiFacetKeyPojo found = dao.find().eq("partKey").eq("clust1").eq("clust2").build().execute("part1", "clust1", "clust2").one();
        assertNotNull(found);
        assertEquals("part1", found.getPartKey());
        assertEquals("clust1", found.getClust1());
        assertEquals("clust2", found.getClust2());
    }

    @Test
    public void testWithNestedArrayField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();

        pojo.setPojoArray(new NestedPojo[]{new NestedPojo()});
        dao.save(pojo);
        final SimplePojo found = dao.findById(pojo.getId());
        assertArrayEquals(pojo.getPojoArray(), found.getPojoArray());
    }

    @Test
    public void testWithNestedPojoField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setNestedPojo(new NestedPojo());
        dao.save(pojo);
        final SimplePojo found = dao.findById(pojo.getId());
        assertEquals(pojo.getNestedPojo(), found.getNestedPojo());
    }

    @Test
    public void testWithPojoListField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setPojoList(Arrays.asList(new NestedPojo(), new NestedPojo()));
        dao.save(pojo);

        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found.getPojoList());
        assertEquals(2, found.getPojoList().size());
        assertEquals(pojo.getPojoList().get(0), found.getPojoList().get(0));
        assertEquals(pojo.getPojoList().get(1), found.getPojoList().get(1));
    }

    @Test
    public void testWithPojoMapField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        Map<String, NestedPojo> pojoMap = new HashMap<>();
        final NestedPojo nested1 = new NestedPojo();
        final NestedPojo nested2 = new NestedPojo();
        pojoMap.put("one", nested1);
        pojoMap.put("two", nested2);

        pojo.setPojoMap(pojoMap);
        dao.save(pojo);

        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found.getPojoMap());
        assertEquals(2, found.getPojoMap().size());
        assertEquals(nested1, found.getPojoMap().get("one"));
        assertEquals(nested2, found.getPojoMap().get("two"));
    }

    @Test
    public void testWithPojoMapFieldTtl() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setName("NAME");
        Map<String, NestedPojo> pojoMap = new HashMap<>();
        final NestedPojo nested1 = new NestedPojo();
        nested1.setData("DATA");
        final NestedPojo nested2 = new NestedPojo();
        nested2.setData("DATA");
        pojoMap.put("one", nested1);
        pojoMap.put("two", nested2);

        pojo.setPojoMap(pojoMap);
        dao.save(pojo, 96000);

        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found.getPojoMap());
        assertEquals(2, found.getPojoMap().size());
        assertEquals(nested1, found.getPojoMap().get("one"));
        assertEquals(nested2, found.getPojoMap().get("two"));

        ResultSet resultSet2 = getSession().execute("SELECT TTL (name) from simpletons");
        for (Row row : resultSet2) {
            assertTrue(row.getInt(0) <= 96000);
        }

        ResultSet resultSet = getSession().execute("SELECT TTL (data) from nested_pojo");
        for (Row row : resultSet) {
            assertTrue(row.getInt(0) <= 96000 && row.getInt(0) > 0);
        }
    }

    @Test
    public void testWithPojoSetField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        final NestedPojo nested1 = new NestedPojo();
        final NestedPojo nested2 = new NestedPojo();
        pojo.setPojoSet(Sets.newHashSet(nested1, nested2));
        dao.save(pojo);

        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found.getPojoSet());
        assertEquals(2, found.getPojoSet().size());
        assertTrue(found.getPojoSet().contains(nested1));
        assertTrue(found.getPojoSet().contains(nested2));
    }

    @Test
    public void testWithSetField() {
        DefaultPojoDaoFactory factory = getFactory();
        final PojoDao<String, SimplePojo> dao = factory.createPojoDao(SimplePojo.class);
        final SimplePojo pojo = new SimplePojo();
        pojo.setSetOfStrings(Sets.newHashSet("one", "two", "three"));
        dao.save(pojo);
        final SimplePojo found = dao.findById(pojo.getId());
        assertNotNull(found.getSetOfStrings());
        assertEquals(3, found.getSetOfStrings().size());
        assertTrue(found.getSetOfStrings().contains("one"));
        assertTrue(found.getSetOfStrings().contains("two"));
        assertTrue(found.getSetOfStrings().contains("three"));
    }

    @Test
    public void testWithIndexObject() {
        DefaultPojoDaoFactory factory = getFactory();

        PojoDao<Object, PersonByLastFirst> pojoDao = factory.createPojoDao(PersonByLastFirst.class);

        Person person1 = new Person();
        person1.setLastName("World");
        person1.setFirstName("Hello");

        Person person2 = new Person();
        person2.setLastName("World");
        person2.setFirstName("Hello");

        pojoDao.save(new PersonByLastFirst("World", "Hello", person1));
        pojoDao.save(new PersonByLastFirst("World", "Hello", person2));

        List<PersonByLastFirst> found = pojoDao.find().eq("lastName").eq("firstName").build().execute("World", "Hello").list();
        assertNotNull(found);
        assertEquals(2, found.size());
        Set<Person> people = found.stream().map(PersonByLastFirst::getPerson).collect(Collectors.toSet());
        assertTrue(people.contains(person1));
        assertTrue(people.contains(person2));
    }

    @Test
    public void testDurationQuery() {
        DefaultPojoDaoFactory factory = getFactory();

        PojoDao<String, DurationEntity> dao = factory.createPojoDao(DurationEntity.class);

        DurationEntity entity = new DurationEntity("group1", Duration.ofHours(2));
        dao.save(entity);


        List<DurationEntity> found = dao.find().eq("group").gt("duration").build().execute("group1", Duration.ofHours(1)).list();
        assertNotNull(found);
        assertEquals(1, found.size());
    }

    @Test
    public void testInstantQuery() {
        DefaultPojoDaoFactory factory = getFactory();

        PojoDao<String, InstantEntity> dao = factory.createPojoDao(InstantEntity.class);

        Instant now = Instant.now();
        InstantEntity entity = new InstantEntity("group1", now);
        dao.save(entity);


        List<InstantEntity> found = dao.find().eq("group").gt("instant").build().execute("group1", now.minusMillis(10000)).list();
        assertNotNull(found);
        assertEquals(1, found.size());
    }

    @Test
    public void testLocalDateQuery() {
        PojoDao<String, LocalDateEntity> dao = getFactory().createPojoDao(LocalDateEntity.class);
        dao.save(new LocalDateEntity("group1", LocalDate.now()));

        List<LocalDateEntity> found = dao.find().eq("group").lt("date").build().execute("group1", LocalDate.now().plusDays(2)).list();
        assertNotNull(found);
        assertEquals(1, found.size());
    }

    @Test
    public void testLocalDateTimeQuery() {
        PojoDao<String, LocalDateTimeEntity> dao = getFactory().createPojoDao(LocalDateTimeEntity.class);
        dao.save(new LocalDateTimeEntity("group1", LocalDateTime.now()));

        List<LocalDateTimeEntity> found = dao.find().eq("group").lt("date").build().execute("group1", LocalDateTime.now().plusDays(2)).list();
        assertNotNull(found);
        assertEquals(1, found.size());
    }

    @Test
    public void testLocalTimeQuery() {
        PojoDao<String, LocalTimeEntity> dao = getFactory().createPojoDao(LocalTimeEntity.class);
        dao.save(new LocalTimeEntity("group1", LocalTime.now()));

        List<LocalTimeEntity> found = dao.find().eq("group").lt("time").build().execute("group1", LocalTime.now().plusHours(2)).list();
        assertNotNull(found);
        assertEquals(1, found.size());
    }

    @Test
    public void testOffsetDateTimeQuery() {
        PojoDao<String, OffsetDateTimeEntity> dao = getFactory().createPojoDao(OffsetDateTimeEntity.class);
        dao.save(new OffsetDateTimeEntity("group1", OffsetDateTime.now()));

        List<OffsetDateTimeEntity> found = dao.find().eq("group").lt("date").build().execute("group1", OffsetDateTime.now().plusDays(2)).list();
        assertNotNull(found);
        assertEquals(1, found.size());
    }

    @Test
    public void testOffsetTimeQuery() {
        PojoDao<String, OffsetTimeEntity> dao = getFactory().createPojoDao(OffsetTimeEntity.class);
        dao.save(new OffsetTimeEntity("group1", OffsetTime.now()));

        List<OffsetTimeEntity> found = dao.find().eq("group").lt("time").build().execute("group1", OffsetTime.now().plusHours(2)).list();
        assertNotNull(found);
        assertEquals(1, found.size());
    }

    @Test
    public void testPeriodQuery() {
        PojoDao<String, PeriodEntity> dao = getFactory().createPojoDao(PeriodEntity.class);

        PeriodEntity entity = new PeriodEntity("group1", Period.ofYears(2));
        dao.save(entity);

        List<PeriodEntity> found = dao.find().eq("group").gt("period").build().execute("group1", Period.ofYears(1)).list();
        assertNotNull(found);
        assertEquals(1, found.size());
    }

}