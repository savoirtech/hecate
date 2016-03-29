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

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.BindingTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Before;
import org.junit.Test;

public class CompositeKeyBindingTest extends BindingTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private CompositeKeyBinding binding;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void setUp() {
        this.binding = new CompositeKeyBinding(Lists.newArrayList(getFacet(CompositeKeyEntity.class, "pk"), getFacet(CompositeKeyEntity.class, "cluster")), getNamingStrategy(), getConverterRegistry(), getBindingFactory());
    }

    @Test(expected = HecateException.class)
    @Cassandra
    public void testClusteringColumnReference() {
        createPojoDao(ClusteringColumnReferenceEntity.class);
    }

    @Test
    public void testCreate() {
        assertCreateEquals(binding, "CREATE TABLE foo( pk varchar, cluster varchar, PRIMARY KEY(pk, cluster))");
    }

    @Test
    public void testDelete() {
        assertDeleteEquals(binding, "DELETE FROM foo WHERE pk=? AND cluster=?;");
    }

    @Test
    @Cassandra
    public void testElementReference() {
        PojoDao<ReferencingElementEntity> dao = createPojoDao(ReferencingElementEntity.class);
        ReferencingElementEntity entity = new ReferencingElementEntity();
        CompositeKeyEntity ref1 = new CompositeKeyEntity("1", "2");
        CompositeKeyEntity ref2 = new CompositeKeyEntity("1", "3");
        entity.setRefs(Lists.newArrayList(ref1, ref2));

        dao.save(entity);

        ReferencingElementEntity found = dao.findByKey(entity.getId());
        assertEquals(Arrays.asList(ref1, ref2), found.getRefs());
    }

    @Test
    public void testInsert() {
        assertInsertEquals(binding, "INSERT INTO foo(pk,cluster) VALUES (?,?);");
    }

    @Test
    @Cassandra
    public void testReferenceFacet() {
        PojoDao<ReferencingEntity> dao = createPojoDao(ReferencingEntity.class);
        ReferencingEntity entity = new ReferencingEntity();
        CompositeKeyEntity ref = new CompositeKeyEntity("1", "2");
        entity.setRef(ref);
        dao.save(entity);

        ReferencingEntity found = dao.findByKey(entity.getId());
        assertEquals(entity, found);
        assertNotNull(found.getRef());
    }

    @Test
    @Cassandra
    public void testReferenceFacetWithNullReference() {
        PojoDao<ReferencingEntity> dao = createPojoDao(ReferencingEntity.class);
        ReferencingEntity entity = new ReferencingEntity();
        dao.save(entity);

        ReferencingEntity found = dao.findByKey(entity.getId());
        assertEquals(entity, found);
        assertNull(found.getRef());
    }

    @Test
    public void testSelect() {
        assertSelectEquals(binding, "SELECT pk,cluster FROM foo;");
    }

    @Test
    public void testSelectWhere() {
        assertSelectWhereEquals(binding, "SELECT pk,cluster FROM foo WHERE pk=? AND cluster=?;");
    }

    @Test
    public void testWithNoConverterForPartitionKey() {
        assertHecateException("No converter found for @PartitionKey facet \"key\".", () -> getBindingFactory().createPojoBinding(NoConverterForKeyEntity.class));
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class ClusteringColumnReferenceEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @ClusteringColumn
        private CompositeKeyEntity referenced;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public CompositeKeyEntity getReferenced() {
            return referenced;
        }

        public void setReferenced(CompositeKeyEntity referenced) {
            this.referenced = referenced;
        }
    }

    public static class CompositeKeyEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @PartitionKey
        private String pk;

        @ClusteringColumn
        private String cluster;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public CompositeKeyEntity(String pk, String cluster) {
            this.pk = pk;
            this.cluster = cluster;
        }

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

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CompositeKeyEntity that = (CompositeKeyEntity) o;

            if (pk != null ? !pk.equals(that.pk) : that.pk != null) return false;
            return cluster != null ? cluster.equals(that.cluster) : that.cluster == null;
        }

        @Override
        public int hashCode() {
            int result = pk != null ? pk.hashCode() : 0;
            result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
            return result;
        }
    }

    public static class NoConverterForKeyEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @PartitionKey
        private Serializable key;

        @ClusteringColumn
        private String cluster;
    }

    public static class ReferencingElementEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private List<CompositeKeyEntity> refs = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public List<CompositeKeyEntity> getRefs() {
            return refs;
        }

        public void setRefs(List<CompositeKeyEntity> refs) {
            this.refs = refs;
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