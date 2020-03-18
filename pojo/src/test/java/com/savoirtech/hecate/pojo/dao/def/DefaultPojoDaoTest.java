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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.savoirtech.hecate.test.CassandraSingleton;
import java.util.concurrent.TimeUnit;

import com.savoirtech.hecate.annotation.Ttl;
import com.savoirtech.hecate.core.statement.StatementOptionsBuilder;
import com.savoirtech.hecate.core.update.AsyncUpdateGroup;
import com.savoirtech.hecate.core.update.BatchUpdateGroup;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import org.junit.Before;
import org.junit.Test;

public class DefaultPojoDaoTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private PojoDao<Person> dao;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void setUp() throws Exception {
        dao = createPojoDao(Person.class);
    }

    @Test
    public void testSave() {
        Person expected = new Person("Slappy", "White");
        dao.save(expected);

        Person actual = dao.findByKey(expected.getId());
        assertEquals(expected, actual);
    }

    @Test
    public void testSaveWithSimpleDaoFactory() {
        Person expected = new Person("Slappy", "White");
        DefaultPojoDaoFactoryBuilder factory = new DefaultPojoDaoFactoryBuilder(CassandraSingleton.getSession());
        PojoDao<Person> dao = factory.build().createPojoDao(Person.class);
        dao.save(expected);
        Person actual = dao.findByKey(expected.getId());
        assertEquals(expected, actual);
    }

    @Test
    public void testSaveWithUpdateGroup() {
        Person expected = new Person("Slappy", "White");
        BatchUpdateGroup group = new BatchUpdateGroup(CassandraSingleton.getSession(), EXECUTOR);
        dao.save(group, expected);
        assertNull(dao.findByKey(expected.getId()));
        group.complete();
        assertEquals(expected, dao.findByKey(expected.getId()));
    }

    @Test
    public void testSaveWithUpdateGroupAndOptions() {
        TtlEntity expected = new TtlEntity();
        PojoDao<TtlEntity> dao = createPojoDao(TtlEntity.class);
        AsyncUpdateGroup group = new AsyncUpdateGroup(CassandraSingleton.getSession());
        long ts = System.currentTimeMillis() + 20000;
        dao.save(group, StatementOptionsBuilder.defaultTimestamp(ts).build(), expected);
        group.complete();
        assertEquals(expected, dao.findByKey(expected.getId()));
        assertEquals(ts, writeTime(expected));
    }


    @Test
    public void testSaveWithUpdateGroupAndOptionsAndTtl() {
        TtlEntity expected = new TtlEntity();
        PojoDao<TtlEntity> dao = createPojoDao(TtlEntity.class);
        AsyncUpdateGroup group = new AsyncUpdateGroup(CassandraSingleton.getSession());
        long ts = System.currentTimeMillis() + 20000;
        dao.save(group, StatementOptionsBuilder.defaultTimestamp(ts).build(), expected, 60000);
        group.complete();
        assertEquals(expected, dao.findByKey(expected.getId()));
        assertEquals(ts, writeTime(expected));
        assertTrue(ttlOf(expected) > 30000);
    }

    @Test
    public void testSaveWithUpdateGroupAndTtl() {
        TtlEntity expected = new TtlEntity();
        PojoDao<TtlEntity> dao = createPojoDao(TtlEntity.class);
        AsyncUpdateGroup group = new AsyncUpdateGroup(CassandraSingleton.getSession());
        long ts = System.currentTimeMillis() + 20000;
        dao.save(group, expected, 60000);
        group.complete();
        assertEquals(expected, dao.findByKey(expected.getId()));
        assertTrue(ttlOf(expected) > 30000);
    }




    private int ttlOf(TtlEntity entity) {
        SimpleStatement query = QueryBuilder.selectFrom("ttl_entity").ttl("name").whereColumn("id").isEqualTo(literal(entity.getId())).build();
        return CassandraSingleton.getSession().execute(query).one().getInt(0);
    }

    private long writeTime(TtlEntity entity) {
        SimpleStatement query = QueryBuilder.selectFrom("ttl_entity").writeTime("name").whereColumn("id").isEqualTo(literal(entity.getId())).build();
        return CassandraSingleton.getSession().execute(query).one().getLong(0);
    }

    @Test
    public void testSaveWithTtlAnnotation() {
        TtlEntity entity = new TtlEntity();
        createPojoDao(TtlEntity.class).save(entity);
        assertTrue(ttlOf(entity) > 0);
    }

    @Test
    public void testSaveWithTtlOverride() {
        TtlEntity entity = new TtlEntity();
        createPojoDao(TtlEntity.class).save(entity, 60000);
        assertTrue(ttlOf(entity) > 30000);
    }

    @Test
    public void testSaveWithStatementOptions() {
        long now = System.currentTimeMillis() + 200000;
        TtlEntity entity = new TtlEntity();
        createPojoDao(TtlEntity.class).save(StatementOptionsBuilder.defaultTimestamp(now).build(), entity);
        assertEquals(now, writeTime(entity));
    }

    @Test
    public void testSaveWithStatementOptionsAndTtl() {
        long now = System.currentTimeMillis() + 200000;
        TtlEntity entity = new TtlEntity();
        createPojoDao(TtlEntity.class).save(StatementOptionsBuilder.defaultTimestamp(now).build(), entity, 60000);
        assertEquals(now, writeTime(entity));
        assertTrue(ttlOf(entity) > 30000);
    }

    @Test
    public void testSaveWithTtlAnnotationAsync() throws Exception {
        TtlEntity entity = new TtlEntity();
        createPojoDao(TtlEntity.class).saveAsync(entity).get();
        assertTrue(ttlOf(entity) > 0);
    }

    @Test
    public void testSaveWithTtlOverrideAsync() throws Exception {
        TtlEntity entity = new TtlEntity();
        createPojoDao(TtlEntity.class).saveAsync(entity, 60000).get(5, TimeUnit.SECONDS);
        assertTrue(ttlOf(entity) > 30000);
    }

    @Test
    public void testSaveWithStatementOptionsAsync() throws Exception {
        long now = System.currentTimeMillis() + 200000;
        TtlEntity entity = new TtlEntity();
        createPojoDao(TtlEntity.class).saveAsync(StatementOptionsBuilder.defaultTimestamp(now).build(), entity).get(5, TimeUnit.SECONDS);
        assertEquals(now, writeTime(entity));
    }

    @Test
    public void testSaveWithStatementOptionsAndTtlAsync() throws Exception {
        long now = System.currentTimeMillis() + 200000;
        TtlEntity entity = new TtlEntity();
        createPojoDao(TtlEntity.class).saveAsync(StatementOptionsBuilder.defaultTimestamp(now).build(), entity, 60000).get(5, TimeUnit.SECONDS);
        assertEquals(now, writeTime(entity));
        assertTrue(ttlOf(entity) > 30000);
    }

    @Test
    public void testDeletePojo() {
        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.delete(pojo);
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testDeleteNullPojo() {
        PojoDao<Person> dao = createPojoDao(Person.class);
        dao.delete(null);
    }

    @Test
    public void testDeletePojoWithOptions() {
        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.delete(StatementOptionsBuilder.consistencyLevel(ConsistencyLevel.ALL).build(), pojo);
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testDeletePojoAsync() throws Exception {
        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.deleteAsync(pojo).get();
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testDeletePojoWithOptionsAsync() throws Exception {
        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.deleteAsync(StatementOptionsBuilder.consistencyLevel(ConsistencyLevel.ALL).build(), pojo).get();
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testDeleteWithGroup() throws Exception {

        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        AsyncUpdateGroup asyncUpdateGroup = new AsyncUpdateGroup(CassandraSingleton.getSession());
        dao.delete(asyncUpdateGroup, pojo);
        asyncUpdateGroup.complete();
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testDeletePojoByKey() {
        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.deleteByKey(pojo.getId());
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testDeletePojoByKeyWhenNotFound() {
        PojoDao<Person> dao = createPojoDao(Person.class);
        dao.deleteByKey("bogus");
    }

    @Test
    public void testDeletePojoByKeyWithOptions() {
        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.deleteByKey(StatementOptionsBuilder.consistencyLevel(ConsistencyLevel.ALL).build(), pojo.getId());
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testDeletePojoByKeyAsync() throws Exception {
        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.deleteByKeyAsync(pojo.getId()).get();
        assertNull(dao.findByKey(pojo.getId()));
    }

    @Test
    public void testDeletePojoByKeyWithOptionsAsync() throws Exception {
        PojoDao<Person> dao = createPojoDao(Person.class);
        Person pojo = new Person("Slappy", "White");
        dao.save(pojo);
        assertNotNull(dao.findByKey(pojo.getId()));
        dao.deleteByKeyAsync(StatementOptionsBuilder.consistencyLevel(ConsistencyLevel.ALL).build(), pojo.getId()).get();
        assertNull(dao.findByKey(pojo.getId()));
    }
    

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    @Ttl(30000)
    public static class TtlEntity extends UuidEntity {
        private String name = "foo";

    }

    public static class Person extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final String firstName;
        private final String lastName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public Person(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }
    }
}