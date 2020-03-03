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

package com.savoirtech.hecate.pojo.binding.facet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Set;

import com.google.common.collect.Sets;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import org.junit.Test;

public class SetFacetBindingTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testWithNullPojoSet() {
        PojoDao<PojoSetEntity> dao = createPojoDao(PojoSetEntity.class);
        PojoSetEntity entity = new PojoSetEntity();
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertNull(entity.getPojos());
    }

    @Test
    public void testWithNullPrimitiveSet() {
        PojoDao<PrimitiveSetEntity> dao = createPojoDao(PrimitiveSetEntity.class);
        PrimitiveSetEntity entity = new PrimitiveSetEntity();
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertNull(entity.getInts());
    }

    @Test
    public void testWithPojoSet() {
        PojoDao<PojoSetEntity> dao = createPojoDao(PojoSetEntity.class);
        PojoSetEntity entity = new PojoSetEntity();
        Set<ElementEntity> expected = Sets.newHashSet(new ElementEntity(), new ElementEntity());
        entity.setPojos(expected);
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertEquals(expected, entity.getPojos());
    }

    @Test
    public void testWithPrimitiveSet() {
        PojoDao<PrimitiveSetEntity> dao = createPojoDao(PrimitiveSetEntity.class);
        PrimitiveSetEntity entity = new PrimitiveSetEntity();
        Set<Integer> expected = Sets.newHashSet(3, 1, 4, 1, 5, 9);
        entity.setInts(expected);
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertEquals(expected, entity.getInts());
    }

    @Test
    public void testWithNullElement() {
        assertHecateException("Cassandra driver does not support null values inside Set(INT, not frozen) collections.", () -> {
            PojoDao<PrimitiveSetEntity> dao = createPojoDao(PrimitiveSetEntity.class);
            PrimitiveSetEntity entity = new PrimitiveSetEntity();
            entity.setInts(Sets.newHashSet(1, null, 3));
            dao.save(entity);
        });

    }
//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class PojoSetEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private Set<ElementEntity> pojos;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public Set<ElementEntity> getPojos() {
            return pojos;
        }

        public void setPojos(Set<ElementEntity> pojos) {
            this.pojos = pojos;
        }
    }

    public static class PrimitiveSetEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private Set<Integer> ints;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public Set<Integer> getInts() {
            return ints;
        }

        public void setInts(Set<Integer> ints) {
            this.ints = ints;
        }
    }
}