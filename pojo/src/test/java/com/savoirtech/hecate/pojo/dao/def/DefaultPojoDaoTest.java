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

import java.util.concurrent.TimeUnit;

import com.savoirtech.hecate.annotation.Ttl;
import com.savoirtech.hecate.core.statement.StatementOptionsBuilder;
import com.savoirtech.hecate.core.update.BatchUpdateGroup;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Before;
import org.junit.Test;

@Cassandra
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
    public void testSaveWithUpdateGroup() {
        Person expected = new Person("Slappy", "White");
        BatchUpdateGroup group = new BatchUpdateGroup(getSession());
        dao.save(group, expected);
        assertNull(dao.findByKey(expected.getId()));
        group.complete();
        assertEquals(expected, dao.findByKey(expected.getId()));
    }

    private int ttlOf(TtlEntity entity) {
        return getSession().execute("select TTL(name) from ttl_entity where id=?", entity.getId()).one().getInt(0);
    }

    private long writeTime(TtlEntity entity) {
        return getSession().execute("select WRITETIME(name) from ttl_entity where id=?", entity.getId()).one().getLong(0);
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