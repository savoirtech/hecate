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

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

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