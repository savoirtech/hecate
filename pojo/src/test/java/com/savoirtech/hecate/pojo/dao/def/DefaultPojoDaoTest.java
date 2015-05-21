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
import com.savoirtech.hecate.annotation.Id;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.def.DefaultPersistenceContext;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultPojoDaoTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testInsert() {
        withSession(session -> {
            DefaultPojoMappingFactory factory = new DefaultPojoMappingFactory(new FieldFacetProvider(), DefaultConverterRegistry.defaultRegistry());
            PojoMapping<Dependent> dependentPojoMapping = factory.createPojoMapping(Dependent.class);
            session.execute(dependentPojoMapping.createCreateStatement());

            PojoMapping<Person> mapping = factory.createPojoMapping(Person.class);
            session.execute(mapping.createCreateStatement());
            PersistenceContext persistenceContext = new DefaultPersistenceContext(session);
            DefaultPojoDao<String, Person> dao = new DefaultPojoDao<>(mapping, persistenceContext);
            Person p = new Person();
            p.firstName = "Slappy";
            p.lastName = "White";
            p.ssn = "123456789";
            dao.save(p);
        });
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class Person {
        @Id
        private String ssn;

        private String lastName;
        private String firstName;

        private Set<String> stringSet = Sets.newHashSet("one", "two", "three");

        private List<String> stringList = Lists.newArrayList("a", "b", "c");
        
        private Map<String,Integer> stringMap = Maps.toMap(Arrays.asList("a", "aa", "aaa"), String::length);

        private String[] stringArray = new String[] {"foo", "bar"};

        private Dependent dependent = new Dependent("bar", "Bar");

        private Set<Dependent> dependentSet = Sets.newHashSet(new Dependent("foo", "Foo"));

        private List<Dependent> dependentList = Lists.newArrayList(new Dependent("baz", "Baz"));

        private Dependent[] dependentArray = new Dependent[] {new Dependent("hello", "world")};
    }

    private static class Dependent {

        @Id
        private String id;

        private String nickname;

        public Dependent(String id, String nickname) {
            this.id = id;
            this.nickname = nickname;
        }
    }
}