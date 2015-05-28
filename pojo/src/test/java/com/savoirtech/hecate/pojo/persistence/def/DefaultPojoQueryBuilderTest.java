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

package com.savoirtech.hecate.pojo.persistence.def;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.CompositeKey;
import com.savoirtech.hecate.pojo.entities.CompositeKeyPojo;
import com.savoirtech.hecate.pojo.entities.QueryablePojo;
import com.savoirtech.hecate.pojo.entities.SimplePojo;
import com.savoirtech.hecate.pojo.persistence.PojoQuery;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class DefaultPojoQueryBuilderTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testAsc() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        UUID partition = UUID.randomUUID();
        dao.save(new QueryablePojo(partition, 124));
        dao.save(new QueryablePojo(partition, 122));
        dao.save(new QueryablePojo(partition, 123));

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition").asc("key.value").build();
        List<Integer> values = query.execute(partition).list().stream().map(pojo -> pojo.getKey().getValue()).collect(Collectors.toList());
        assertEquals(Arrays.asList(122, 123, 124), values);
    }

    @Test
    public void testDesc() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        UUID partition = UUID.randomUUID();
        dao.save(new QueryablePojo(partition, 124));
        dao.save(new QueryablePojo(partition, 122));
        dao.save(new QueryablePojo(partition, 123));

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition").desc("key.value").build();
        List<Integer> values = query.execute(partition).list().stream().map(pojo -> pojo.getKey().getValue()).collect(Collectors.toList());
        assertEquals(Arrays.asList(124, 123, 122), values);
    }

    @Test
    public void testEqualsValue() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition", pojo.getKey().getPartition()).build();
        assertEquals(1, query.execute().list().size());
    }

    @Test
    public void testGreaterThan() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        List<QueryablePojo> results = dao.find().eq("key.partition").gt("key.value").build().execute(pojo.getKey().getPartition(), 4).list();
        assertEquals(1, results.size());
    }

    @Test
    public void testGreaterThanOrEqual() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition").gte("key.value").build();
        assertEquals(1, query.execute(pojo.getKey().getPartition(), 122).list().size());
        assertEquals(1, query.execute(pojo.getKey().getPartition(), 123).list().size());
        assertEquals(0, query.execute(pojo.getKey().getPartition(), 124).list().size());
    }

    @Test
    public void testGreaterThanOrEqualValue() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition").gte("key.value", 123).build();
        assertEquals(1, query.execute(pojo.getKey().getPartition()).list().size());
    }

    @Test
    public void testGreaterThanValue() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        List<QueryablePojo> results = dao.find().eq("key.partition").gt("key.value", 4).build().execute(pojo.getKey().getPartition()).list();
        assertEquals(1, results.size());
    }

    @Test
    public void testIdEquals() {
        final PojoDao<String, SimplePojo> dao = getFactory().createPojoDao(SimplePojo.class);
        SimplePojo pojo = new SimplePojo();
        pojo.setId("123");
        dao.save(pojo);
        SimplePojo found = dao.find().identifierEquals().build().execute("123").one();
        assertNotNull(found);
    }

    @Test
    public void testIdentifierIn() {
        final PojoDao<String, SimplePojo> dao = getFactory().createPojoDao(SimplePojo.class);
        SimplePojo pojo = new SimplePojo();
        pojo.setId("123");
        dao.save(pojo);
        SimplePojo found = dao.find().identifierIn().build().execute(Collections.singletonList("123")).one();
        assertNotNull(found);
    }


    @Test(expected = HecateException.class)
    public void testIdInWhenComposite() {
        PojoDao<CompositeKey, CompositeKeyPojo> dao = getFactory().createPojoDao(CompositeKeyPojo.class);
        dao.find().identifierIn().build();
    }

    @Test
    public void testIn() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition").in("key.value").build();
        assertEquals(1, query.execute(pojo.getKey().getPartition(), Arrays.asList(123, -1, 1)).list().size());
    }

    @Test
    public void testInValue() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition").in("key.value", Arrays.asList(123, -1, 1)).build();
        assertEquals(1, query.execute(pojo.getKey().getPartition()).list().size());
    }

    @Test
    public void testLessThan() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        List<QueryablePojo> results = dao.find().eq("key.partition").lt("key.value").build().execute(pojo.getKey().getPartition(), 124).list();
        assertEquals(1, results.size());
    }

    @Test
    public void testLessThanOrEqual() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition").lte("key.value").build();
        assertEquals(0, query.execute(pojo.getKey().getPartition(), 122).list().size());
        assertEquals(1, query.execute(pojo.getKey().getPartition(), 123).list().size());
        assertEquals(1, query.execute(pojo.getKey().getPartition(), 124).list().size());
    }

    @Test
    public void testLessThanOrEqualValue() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        PojoQuery<QueryablePojo> query = dao.find().eq("key.partition").lte("key.value", 124).build();
        assertEquals(1, query.execute(pojo.getKey().getPartition()).list().size());
    }

    @Test
    public void testLessThanValue() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        QueryablePojo pojo = new QueryablePojo(123);
        dao.save(pojo);

        List<QueryablePojo> results = dao.find().eq("key.partition").lt("key.value", 124).build().execute(pojo.getKey().getPartition()).list();
        assertEquals(1, results.size());
    }

    @Test(expected = HecateException.class)
    public void testWithInvalidFacetName() {
        final PojoDao<UUID, QueryablePojo> dao = getFactory().createPojoDao(QueryablePojo.class);
        dao.find().eq("foo", 123).build();
    }
}