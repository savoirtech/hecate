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

package com.savoirtech.hecate.pojo.binding.key.simple;

import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.google.common.collect.Lists;
import com.savoirtech.hecate.annotation.Cascade;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.test.BindingTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SimpleKeyBindingTest extends BindingTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private SimpleKeyBinding binding;

    @Mock
    private PojoVisitor visitor;

    @Mock
    private Converter converter;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void setUp() throws Exception {
        binding = new SimpleKeyBinding(getFacet(SimpleEntity.class, "id"), "id", converter);
    }

    @Test
    @Cassandra
    public void testClusteringColumnReference() {
        PojoDao<SimpleEntity> refDao = createPojoDao(SimpleEntity.class);
        PojoDao<ClusteringColumnReferenceEntity> dao = createPojoDao(ClusteringColumnReferenceEntity.class);
        ClusteringColumnReferenceEntity entity = new ClusteringColumnReferenceEntity();
        entity.setSimpleEntity(new SimpleEntity());
        dao.save(entity);

        assertNotNull(refDao.findByKey(entity.getSimpleEntity().getId()));

        dao.delete(entity);
        assertNull(refDao.findByKey(entity.getSimpleEntity().getId()));
    }

    @Test
    public void testCreate() {
        when(converter.getDataType()).thenReturn(DataType.varchar());
        assertCreateEquals(binding, "CREATE TABLE IF NOT EXISTS foo( id varchar, PRIMARY KEY(id))");
        verify(converter).getDataType();
    }

    @Test
    public void testDelete() {
        assertDeleteEquals(binding, "DELETE FROM foo WHERE id=?;");
    }

    @Test
    public void testElementToKeys() {
        assertEquals(Collections.singletonList("foo"), binding.elementToKeys("foo"));
    }

    @Test
    public void testGetElementDataType() {
        when(converter.getDataType()).thenReturn(DataType.varchar());
        assertEquals(DataType.varchar(), binding.getElementDataType());
    }

    @Test
    public void testGetElementValue() {
        SimpleEntity entity = new SimpleEntity();
        when(converter.toColumnValue(entity.getId())).thenReturn("2");
        assertEquals("2", binding.getElementValue(entity));
        verify(converter).toColumnValue(entity.getId());
    }

    @Test
    public void testGetKeyParameters() {
        when(converter.toColumnValue("1")).thenReturn("2");
        List<Object> keys = binding.getKeyParameters(Lists.newArrayList("1"));
        assertEquals(Lists.newArrayList("2"), keys);
        verify(converter).toColumnValue("1");
    }

    @Test
    public void testInsert() {
        assertInsertEquals(binding, "INSERT INTO foo(id) VALUES (?);");
    }

    @Test
    @Cassandra
    public void testNonCascadedDelete() {
        PojoDao<SimpleEntity> refDao = createPojoDao(SimpleEntity.class);
        SimpleEntity ref = new SimpleEntity();
        refDao.save(ref);

        PojoDao<NonCascadedReferenceEntity> dao = createPojoDao(NonCascadedReferenceEntity.class);
        NonCascadedReferenceEntity entity = new NonCascadedReferenceEntity();
        entity.setSimpleEntity(ref);
        dao.save(entity);

        dao.delete(entity);

        assertNotNull(refDao.findByKey(ref.getId()));
    }

    @Test
    @Cassandra
    public void testNonCascadedSave() {
        PojoDao<SimpleEntity> refDao = createPojoDao(SimpleEntity.class);
        PojoDao<NonCascadedReferenceEntity> dao = createPojoDao(NonCascadedReferenceEntity.class);
        NonCascadedReferenceEntity entity = new NonCascadedReferenceEntity();
        SimpleEntity ref = new SimpleEntity();
        entity.setSimpleEntity(ref);
        dao.save(entity);

        assertNull(refDao.findByKey(ref.getId()));
    }

    @Test
    @Cassandra
    public void testQuery() {
        PojoDao<SimpleEntity> dao = createPojoDao(SimpleEntity.class);
        SimpleEntity expected = new SimpleEntity();
        dao.save(expected);

        PojoQuery<SimpleEntity> query = dao.find().eq("id").build();
        SimpleEntity actual = query.execute(expected.getId()).one();
        assertEquals(expected, actual);
    }

    @Test
    @Cassandra
    public void testReferenceClusteringColumnReference() {
        PojoDao<ReferenceClusteringColumnReferenceEntity> dao = createPojoDao(ReferenceClusteringColumnReferenceEntity.class);

        ReferenceClusteringColumnReferenceEntity entity = new ReferenceClusteringColumnReferenceEntity();
        ClusteringColumnReferenceEntity reference = new ClusteringColumnReferenceEntity();
        reference.setSimpleEntity(new SimpleEntity());
        entity.setReference(reference);
        dao.save(entity);
    }

    @Test
    @Cassandra
    public void testReferenceFacet() {
        PojoDao<ReferencingEntity> dao = createPojoDao(ReferencingEntity.class);
        ReferencingEntity entity = new ReferencingEntity();
        SimpleEntity ref = new SimpleEntity();
        entity.setSimpleEntity(ref);
        dao.save(entity);

        ReferencingEntity found = dao.findByKey(entity.getId());
        assertEquals(entity, found);
        assertEquals(ref, found.getSimpleEntity());
    }

    @Test
    @Cassandra
    public void testReferenceFacetWithNullReference() {
        PojoDao<ReferencingEntity> dao = createPojoDao(ReferencingEntity.class);
        ReferencingEntity entity = new ReferencingEntity();
        dao.save(entity);
        ReferencingEntity found = dao.findByKey(entity.getId());
        assertEquals(entity, found);
        assertNull(found.getSimpleEntity());
    }

    @Test
    public void testSelect() {
        assertSelectEquals(binding, "SELECT id FROM foo;");
    }

    @Test
    public void testSelectWhere() {
        assertSelectWhereEquals(binding, "SELECT id FROM foo WHERE id=?;");
    }

    @Test
    public void testVisitChildren() {
        binding.visitFacetChildren("foo", facet -> true, visitor);
        Mockito.verifyNoMoreInteractions(visitor);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class ClusteringColumnReferenceEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @ClusteringColumn
        private SimpleEntity simpleEntity;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public SimpleEntity getSimpleEntity() {
            return simpleEntity;
        }

        public void setSimpleEntity(SimpleEntity simpleEntity) {
            this.simpleEntity = simpleEntity;
        }
    }

    public static class CollectionReferenceEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private List<SimpleEntity> entities;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public List<SimpleEntity> getEntities() {
            return entities;
        }

        public void setEntities(List<SimpleEntity> entities) {
            this.entities = entities;
        }
    }

    public static class NonCascadedReferenceEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @Cascade(save = false, delete = false)
        private SimpleEntity simpleEntity;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public SimpleEntity getSimpleEntity() {
            return simpleEntity;
        }

        public void setSimpleEntity(SimpleEntity simpleEntity) {
            this.simpleEntity = simpleEntity;
        }
    }

    public static class ReferenceClusteringColumnReferenceEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private ClusteringColumnReferenceEntity reference;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public ClusteringColumnReferenceEntity getReference() {
            return reference;
        }

        public void setReference(ClusteringColumnReferenceEntity reference) {
            this.reference = reference;
        }
    }

    public static class ReferencingEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private SimpleEntity simpleEntity;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public SimpleEntity getSimpleEntity() {
            return simpleEntity;
        }

        public void setSimpleEntity(SimpleEntity simpleEntity) {
            this.simpleEntity = simpleEntity;
        }
    }

    public static class SimpleEntity extends UuidEntity {
    }
}