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
import com.google.common.collect.Sets;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.def.DefaultPojoDaoFactory;
import com.savoirtech.hecate.pojo.entities.Person;
import com.savoirtech.hecate.pojo.exception.PojoNotFoundException;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMappingFactory;
import com.savoirtech.hecate.pojo.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.pojo.mapping.verify.CreateSchemaVerifier;
import com.savoirtech.hecate.pojo.metrics.PojoMetricsUtils;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.def.DefaultPersistenceContext;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    private PojoDao<String, Person> dao;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void initialize() {
        context = new DefaultPersistenceContext(getSession());
        factory = new DefaultPojoMappingFactory(new CreateSchemaVerifier(getSession()));
        mapping = factory.createPojoMapping(Person.class);
        dao = new DefaultPojoDaoFactory(factory, context).createPojoDao(Person.class);
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
    public void testContains() {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        assertFalse(cache.contains(mapping));
        cache.put(mapping, SSN_1, person1);
        assertTrue(cache.contains(mapping));
    }

    @Test
    public void testIdSet() {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        assertEquals(Collections.emptySet(), cache.idSet(mapping));
        cache.put(mapping, SSN_1, person1);
        assertEquals(Sets.newHashSet(SSN_1), cache.idSet(mapping));
    }

    @Test
    public void testLookupAfterEviction() {
        DefaultPojoCache cache = new DefaultPojoCache(context, 1);
        assertEquals(0, cache.size(mapping));
        assertNotNull(cache.lookup(mapping, SSN_1));
        assertEquals(1, cache.size(mapping));
        assertNotNull(cache.lookup(mapping, SSN_2));
        assertEquals(1, cache.size(mapping));
        assertNotNull(cache.lookup(mapping, SSN_1));
        assertEquals(1, cache.size(mapping));
    }

    @Test
    public void testLookupAfterEvictionWithCustomMaxValues() {
        Map<PojoMapping<?>, Integer> maximums = new HashMap<>();
        maximums.put(mapping, 1);
        DefaultPojoCache cache = new DefaultPojoCache(context, maximums);
        assertEquals(0, cache.size(mapping));
        assertNotNull(cache.lookup(mapping, SSN_1));
        assertEquals(1, cache.size(mapping));
        assertNotNull(cache.lookup(mapping, SSN_2));
        assertEquals(1, cache.size(mapping));
        assertNotNull(cache.lookup(mapping, SSN_1));
        assertEquals(1, cache.size(mapping));
    }

    @Test(expected = PojoNotFoundException.class)
    public void testLookupReturnsNull() {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        cache.lookup(mapping, "foo-bar");
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
    public void testMetricsReporting() {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        Counter counter = PojoMetricsUtils.createCounter(mapping, "cacheMiss");
        assertEquals(0, counter.getCount());
        cache.lookup(mapping, SSN_1);
        assertEquals(1, counter.getCount());
        cache.lookup(mapping, SSN_1);
        assertEquals(1, counter.getCount());
        cache.lookup(mapping, SSN_2);
        assertEquals(2, counter.getCount());
    }

    @Test
    public void testPutAll() {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        cache.putAll(mapping, Sets.newHashSet(person1, null));
        assertEquals(1, cache.size(mapping));
    }

    @Test
    public void testPutNull() {
        DefaultPojoCache cache = new DefaultPojoCache(context);
        cache.put(mapping, "foo", null);
        assertEquals(0, cache.size(mapping));
    }
}