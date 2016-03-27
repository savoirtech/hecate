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

package com.savoirtech.hecate.pojo.query.custom;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Test;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class CustomPojoQueryTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    @Cassandra
    public void testConvertParameters() {
        createTables(QueryEntity.class);
        Select.Where where = QueryBuilder.select("pk").from("query_entity").where(eq("pk", bindMarker()));
        CustomPojoQuery query = new CustomPojoQuery<>(getSession(), getPojoBinding(QueryEntity.class), getContextFactory(), where);
        Object[] params= new Object[] {"one", "two", "three"};
        Object[] converted = query.convertParameters(params);
        assertSame(params, converted);
    }

    @Test
    @Cassandra
    public void testExecute() {
        PojoDao<QueryEntity> dao = createPojoDao(QueryEntity.class);
        PojoQuery<QueryEntity> query = dao.find(where -> where.and(eq("pk", bindMarker())).and(eq("cluster", bindMarker())));
        QueryEntity entity = new QueryEntity();
        entity.setPk("1");
        entity.setCluster("2");
        dao.save(entity);
        QueryEntity found = query.execute("1", "2").one();
        assertNotNull(found);
        assertEquals("1", found.getPk());
        assertEquals("2", found.getCluster());
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class QueryEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @PartitionKey
        private String pk;

        @ClusteringColumn
        private String cluster;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public String getCluster() {
            return cluster;
        }

        public void setCluster(String cluster) {
            this.cluster = cluster;
        }

        public String getPk() {
            return pk;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }
    }
}