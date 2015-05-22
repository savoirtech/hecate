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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.savoirtech.hecate.annotation.*;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.pojo.mapping.verify.CreateSchemaVerifier;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.def.DefaultPersistenceContext;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Test;

import java.util.*;

public class DefaultPojoDaoTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testInsert() {
        withSession(session -> {
            DefaultPojoMappingFactory factory = new DefaultPojoMappingFactory(new FieldFacetProvider(), DefaultConverterRegistry.defaultRegistry());
            factory.setVerifier(new CreateSchemaVerifier(session));
            PojoMapping<Person> mapping = factory.createPojoMapping(Person.class);
            PersistenceContext persistenceContext = new DefaultPersistenceContext(session,factory, Collections.emptyList());
            DefaultPojoDao<String, Person> dao = new DefaultPojoDao<>(mapping, persistenceContext);
            Person p = new Person();
            p.firstName = "Slappy";
            p.lastName = "White";
            p.ssn = "123456789";
            p.stringSet = Sets.newHashSet("one", "two", "three");
            p.stringList = Lists.newArrayList("a", "b", "c");
            p.stringMap = Maps.toMap(Arrays.asList("a", "aa", "aaa"), String::length);
            p.stringArray = new String[]{"foo", "bar"};
            p.dependent = new Dependent("bar", "Bar");
            p.dependentSet = Sets.newHashSet(new Dependent("foo", "Foo"));
            p.dependentList = Lists.newArrayList(new Dependent("baz", "Baz"));
            p.dependentArray = new Dependent[]{new Dependent("hello", "world")};
            p.dependentMap = Maps.toMap(Arrays.asList("one", "two", "three"), str -> new Dependent(str,str + "_nick"));

            Address home = new Address();
            home.address1 = "123 Main St.";
            home.address2 = "Apt. A";
            home.city = "Chestertonfieldville";
            home.state = "IA";
            home.zip = "12345";
            p.home = home;
            dao.save(p);

            Person found = dao.findById("123456789");
            assertNotNull(found);
            assertEquals("Slappy", found.firstName);
            dao.delete("123456789");
            assertNull(dao.findById("123456789"));
        });
    }

    @Test
    public void testInsertWithEmbeddedKey() {
        withSession(session -> {
            DefaultPojoMappingFactory factory = new DefaultPojoMappingFactory(new FieldFacetProvider(), DefaultConverterRegistry.defaultRegistry());
            factory.setVerifier(new CreateSchemaVerifier(session));
            PojoMapping<WithEmbeddedKey> mapping = factory.createPojoMapping(WithEmbeddedKey.class);
            PersistenceContext persistenceContext = new DefaultPersistenceContext(session,factory, Collections.emptyList());
            PojoDao<EmbeddedKey, WithEmbeddedKey> dao = new DefaultPojoDao<EmbeddedKey, WithEmbeddedKey>(mapping, persistenceContext);
            WithEmbeddedKey pojo = new WithEmbeddedKey();
            pojo.key = new EmbeddedKey();
            pojo.key.cluster = "cluster1";
            pojo.key.partition1 = "partition1Value";
            pojo.key.partition2 = "partition2Value";
            pojo.foo = "Foo";
            pojo.bar = "Bar";
            dao.save(pojo);
        });
    }

    @Test
    public void testWithoutCascading() {
        withSession(session -> {
            DefaultPojoMappingFactory factory = new DefaultPojoMappingFactory(new FieldFacetProvider(), DefaultConverterRegistry.defaultRegistry());
            factory.setVerifier(new CreateSchemaVerifier(session));
            PojoMapping<PersonWithoutCascade> mapping = factory.createPojoMapping(PersonWithoutCascade.class);
            PersistenceContext persistenceContext = new DefaultPersistenceContext(session,factory, Collections.emptyList());
            PojoDao<String, PersonWithoutCascade> dao = new DefaultPojoDao<String, PersonWithoutCascade>(mapping, persistenceContext);
            PersonWithoutCascade p = new PersonWithoutCascade();
            p.firstName = "Slappy";
            p.lastName = "White";
            p.ssn = "123456789";
            p.dependents = Sets.newHashSet(new Dependent("foo", "bar"));
            dao.save(p);

            PersonWithoutCascade found = dao.findById("123456789");
            assertNotNull(found);
            assertTrue(found.dependents.isEmpty());

        });
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class PersonWithoutCascade {
        @Id
        private String ssn;

        private String firstName;
        private String lastName;

        @Cascade(save = false)
        @Table("foo")
        private Set<Dependent> dependents;
    }

    public static class Dependent {
        @Id
        private String id;

        private String nickname;

        public Dependent() {

        }

        public String toString() {
            return nickname;
        }

        public Dependent(String id, String nickname) {
            this.id = id;
            this.nickname = nickname;
        }
    }

    public static class Person {
        @Id
        private String ssn;

        @Embedded
        private Address home;

        @Embedded
        private Address business;

        private String lastName;
        private String firstName;

        private Set<String> stringSet;

        private List<String> stringList;

        private Map<String, Integer> stringMap;

        private String[] stringArray;

        private Dependent dependent;

        private Set<Dependent> dependentSet;

        private List<Dependent> dependentList;

        private Map<String,Dependent> dependentMap;

        private Dependent[] dependentArray;
    }

    public static class Address {
        private String address1;
        private String address2;
        private String city;
        private String state;
        private String zip;

    }

    public static class EmbeddedKey {
        @PartitionKey
        private String partition1;

        @PartitionKey
        private String partition2;

        @ClusteringColumn
        private String cluster;
    }

    public static class WithEmbeddedKey {
        @Id
        private EmbeddedKey key;

        private String foo;
        private String bar;

    }
}