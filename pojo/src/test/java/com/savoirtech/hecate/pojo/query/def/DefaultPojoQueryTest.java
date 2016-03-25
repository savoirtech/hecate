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

import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Test;

public class DefaultPojoQueryTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    @Cassandra
    public void testConvertParameters() {
        PojoDao<QueryEntity> dao = createPojoDao(QueryEntity.class);
        PojoQuery<QueryEntity> query = dao.find().eq("pk").eq("cluster").build();

        OffsetDateTime now = OffsetDateTime.now();

        QueryEntity entity = new QueryEntity();
        entity.setPk("one");
        entity.setCluster(now);
        dao.save(entity);

        QueryEntity found = query.execute("one", now).one();
        assertNotNull(found);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class QueryEntity {
        @PartitionKey
        private String pk;

        @ClusteringColumn
        private OffsetDateTime cluster;

        public String getPk() {
            return pk;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }

        public OffsetDateTime getCluster() {
            return cluster;
        }

        public void setCluster(OffsetDateTime cluster) {
            this.cluster = cluster;
        }
    }
}