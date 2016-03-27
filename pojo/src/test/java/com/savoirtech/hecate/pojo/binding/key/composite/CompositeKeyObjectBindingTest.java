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

package com.savoirtech.hecate.pojo.binding.key.composite;

import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.EmbeddedKey;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.BindingTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Before;
import org.junit.Test;

public class CompositeKeyObjectBindingTest extends BindingTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private CompositeKeyObjectBinding binding;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void setUp() {
        binding = new CompositeKeyObjectBinding(getFacet(CompositeKeyEntity.class, "key"), getFacetProvider(), getConverterRegistry(), getNamingStrategy(), getBindingFactory());
    }

    @Test
    public void testCreate() {
        assertCreateEquals(binding, "CREATE TABLE foo( key_pk varchar, key_cluster varchar, PRIMARY KEY(key_pk, key_cluster))");
    }

    @Test
    public void testDelete() {
        assertDeleteEquals(binding, "DELETE FROM foo WHERE key_pk=? AND key_cluster=?;");
    }

    @Test
    public void testInsert() {
        assertInsertEquals(binding, "INSERT INTO foo(key_pk,key_cluster) VALUES (?,?);");
    }

    @Test
    public void testSelect() {
        assertSelectEquals(binding, "SELECT key_pk,key_cluster FROM foo;");
    }

    @Test
    public void testSelectWhere() {
        assertSelectWhereEquals(binding, "SELECT key_pk,key_cluster FROM foo WHERE key_pk=? AND key_cluster=?;");
    }

    @Test
    @Cassandra
    public void testReferenceBinding() {
        createTables(CompositeKeyEntity.class);
        PojoDao<ReferencingEntity> dao = createPojoDao(ReferencingEntity.class);

        ReferencingEntity entity = new ReferencingEntity();
        CompositeKeyEntity ref = new CompositeKeyEntity();
        KeyObject key = new KeyObject();
        key.setPk("1");
        key.setCluster("2");
        ref.setKey(key);
        entity.setRef(ref);

        dao.save(entity);

        ReferencingEntity found = dao.findByKey(entity.getId());
        assertNotNull(found);
        assertNotNull(found.getRef());
        assertNotNull(found.getRef().getKey());
        assertEquals("1", found.getRef().getKey().getPk());
        assertEquals("2", found.getRef().getKey().getCluster());
    }

    @Test
    @Cassandra
    public void testReferenceBindingWithNullReference() {
        createTables(CompositeKeyEntity.class);
        PojoDao<ReferencingEntity> dao = createPojoDao(ReferencingEntity.class);

        ReferencingEntity entity = new ReferencingEntity();
        dao.save(entity);

        ReferencingEntity found = dao.findByKey(entity.getId());
        assertNotNull(found);
        assertNull(found.getRef());

    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class CompositeKeyEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @EmbeddedKey
        private KeyObject key;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public KeyObject getKey() {
            return key;
        }

        public void setKey(KeyObject key) {
            this.key = key;
        }
    }

    public static class KeyObject {
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

    public static class ReferencingEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private CompositeKeyEntity ref;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public CompositeKeyEntity getRef() {
            return ref;
        }

        public void setRef(CompositeKeyEntity ref) {
            this.ref = ref;
        }
    }
}