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

package com.savoirtech.hecate.pojo.cache.def;

import com.codahale.metrics.Counter;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.pojo.entities.Person;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMappingFactory;
import com.savoirtech.hecate.pojo.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.pojo.mapping.verify.CreateSchemaVerifier;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.def.DefaultPersistenceContext;
import com.savoirtech.hecate.pojo.util.PojoMetricsUtils;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Before;
import org.junit.Test;

public class DefaultPojoCacheTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final String SSN_1 = "123456789";
    public static final String SSN_2 = "987654321";
    private PersistenceContext context;
    private PojoMappingFactory factory;
    private PojoMapping<Person> mapping;
    private Person person1;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void initialize() {
        context = new DefaultPersistenceContext(getSession());
        factory = new DefaultPojoMappingFactory(new CreateSchemaVerifier(getSession()));
        mapping = factory.createPojoMapping(Person.class);
        PojoDao<String,Person> dao = new DefaultPojoDaoFactory(factory,context).createPojoDao(Person.class);
        person1 = new Person();
        person1.setFirstName("Slappy");
        person1.setLastName("White");
        person1.setSsn(SSN_1);
        dao.save(person1);

        Person person2 = new Person();
        person2.setFirstName("Prickly");
        person2.setLastName("Pete");
        person2.setSsn(SSN_2);
        dao.save(person2);
    }

    @Test
    public void testLookupWithCacheHit() throws Exception {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        cache.put(mapping, SSN_1, person1);
        Person person = cache.lookup(mapping, SSN_1);
        assertNotNull(person);
    }

    @Test
    public void testLookupWithCacheMiss() throws Exception {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        Person person = cache.lookup(mapping, SSN_1);
        assertNotNull(person);
    }

    @Test
    public void testLookupAfterEviction() {
        DefaultPojoCache cache = new DefaultPojoCache(context,1);
        assertEquals(0, cache.size(mapping));
        assertNotNull(cache.lookup(mapping,SSN_1));
        assertEquals(1, cache.size(mapping));
        assertNotNull(cache.lookup(mapping, SSN_2));
        assertEquals(1, cache.size(mapping));
        assertNotNull(cache.lookup(mapping, SSN_1));
        assertEquals(1, cache.size(mapping));
    }

    @Test
    public void testMetricsReporting() {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        Counter counter = PojoMetricsUtils.createCounter(mapping, "cacheMiss");
        assertEquals(0,counter.getCount());
        cache.lookup(mapping, SSN_1);
        assertEquals(1,counter.getCount());
        cache.lookup(mapping, SSN_1);
        assertEquals(1,counter.getCount());
        cache.lookup(mapping, SSN_2);
        assertEquals(2,counter.getCount());
    }
}