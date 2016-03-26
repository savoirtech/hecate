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
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
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
    public void testCreate() {
        when(converter.getDataType()).thenReturn(DataType.varchar());
        assertCreateEquals(binding, "CREATE TABLE foo( id varchar, PRIMARY KEY(id))");
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

    @Test
    @Cassandra
    public void testReferenceFacet() {
        createTables(SimpleEntity.class);
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
        createTables(SimpleEntity.class);
        PojoDao<ReferencingEntity> dao = createPojoDao(ReferencingEntity.class);
        ReferencingEntity entity = new ReferencingEntity();
        dao.save(entity);
        ReferencingEntity found = dao.findByKey(entity.getId());
        assertEquals(entity, found);
        assertNull(found.getSimpleEntity());
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

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