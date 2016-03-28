/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.query.def;

import java.time.OffsetDateTime;
import java.util.List;

import com.google.common.collect.Lists;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.query.PojoMultiQuery;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Before;
import org.junit.Test;

@Cassandra
public class DefaultPojoQueryBuilderTest extends AbstractDaoTestCase {
    public static final String PARTITION_KEY_VALUE = "pk";
    //----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private PojoDao<QueryPojo> dao;
    private OffsetDateTime now;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void setUp() {
        this.dao = createPojoDao(QueryPojo.class);
        this.now = OffsetDateTime.now();
        dao.save(new QueryPojo(PARTITION_KEY_VALUE, now));
        dao.save(new QueryPojo(PARTITION_KEY_VALUE, now.plusDays(1)));
        dao.save(new QueryPojo(PARTITION_KEY_VALUE, now.plusDays(2)));
    }

    @Test
    public void testAsc() {
        List<QueryPojo> found = dao.find().eq("key", PARTITION_KEY_VALUE).asc("timestamp").build().execute().list();
        assertEquals(3, found.size());
        assertEquals(now, found.get(0).getTimestamp());
        assertEquals(now.plusDays(1), found.get(1).getTimestamp());
        assertEquals(now.plusDays(2), found.get(2).getTimestamp());
    }

    @Test
    public void testDesc() {
        List<QueryPojo> found = dao.find().eq("key", PARTITION_KEY_VALUE).desc("timestamp").build().execute().list();
        assertEquals(3, found.size());
        assertEquals(now, found.get(2).getTimestamp());
        assertEquals(now.plusDays(1), found.get(1).getTimestamp());
        assertEquals(now.plusDays(2), found.get(0).getTimestamp());
    }
    @Test
    public void testEq() {
        List<QueryPojo> found = dao.find().eq("key").eq("timestamp").build().execute(PARTITION_KEY_VALUE, now.plusDays(1)).list();
        assertEquals(1, found.size());
    }

    @Test
    public void testEqConstant() {
        List<QueryPojo> found = dao.find().eq("key").eq("timestamp", now.plusDays(1)).build().execute(PARTITION_KEY_VALUE).list();
        assertEquals(1, found.size());
    }

    @Test
    public void testGt() {
        List<QueryPojo> found = dao.find().eq("key").gt("timestamp").build().execute(PARTITION_KEY_VALUE, now).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testGtConstant() {
        List<QueryPojo> found = dao.find().eq("key").gt("timestamp", now).build().execute(PARTITION_KEY_VALUE).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testGte() {
        List<QueryPojo> found = dao.find().eq("key").gte("timestamp").build().execute(PARTITION_KEY_VALUE, now.plusDays(1)).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testGteConstant() {
        List<QueryPojo> found = dao.find().eq("key").gte("timestamp", now.plusDays(1)).build().execute(PARTITION_KEY_VALUE).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testLt() {
        List<QueryPojo> found = dao.find().eq("key").lt("timestamp").build().execute(PARTITION_KEY_VALUE, now.plusDays(2)).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testLtConstant() {
        List<QueryPojo> found = dao.find().eq("key").lt("timestamp", now.plusDays(2)).build().execute(PARTITION_KEY_VALUE).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testLte() {
        List<QueryPojo> found = dao.find().eq("key").lte("timestamp").build().execute(PARTITION_KEY_VALUE, now.plusDays(1)).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testLteConstant() {
        List<QueryPojo> found = dao.find().eq("key").lte("timestamp", now.plusDays(1)).build().execute(PARTITION_KEY_VALUE).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testIn() {
        List<QueryPojo> found = dao.find().eq("key").in("timestamp").build().execute(PARTITION_KEY_VALUE, Lists.newArrayList(now, now.plusDays(1), now.plusDays(4))).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testInWithNonIterableParameter() {
        assertHecateException("Invalid parameter type (java.time.OffsetDateTime) for IN expression, java.lang.Iterable<Object> required.", () -> dao.find().eq("key").in("timestamp").build().execute(PARTITION_KEY_VALUE, now));
    }

    @Test
    public void testInConstant() {
        List<QueryPojo> found = dao.find().eq("key").in("timestamp", Lists.newArrayList(now, now.plusDays(1), now.plusDays(4))).build().execute(PARTITION_KEY_VALUE).list();
        assertEquals(2, found.size());
    }

    @Test
    public void testWithUnknownFacetName() {
        assertHecateException("Facet \"bogus\" not found.", () -> dao.find().eq("bogus").build());
    }

    @Test
    public void testMulti() {
        PojoMultiQuery<QueryPojo> multi = dao.find().eq("key").eq("timestamp").build().multi();
        List<QueryPojo> found = multi
                .add(PARTITION_KEY_VALUE, now)
                .add(PARTITION_KEY_VALUE, now.plusDays(1))
                .add(PARTITION_KEY_VALUE, now.plusDays(1))
                .execute()
                .list();
        assertEquals(3, found.size());
    }
//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class QueryPojo {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @PartitionKey
        private final String key;

        @ClusteringColumn
        private final OffsetDateTime timestamp;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public QueryPojo(String key, OffsetDateTime timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public String getKey() {
            return key;
        }

        public OffsetDateTime getTimestamp() {
            return timestamp;
        }
    }
}