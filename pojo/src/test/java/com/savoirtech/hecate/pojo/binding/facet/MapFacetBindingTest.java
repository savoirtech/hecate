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

import java.util.Map;

import com.google.common.collect.Maps;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import org.junit.Test;

public class MapFacetBindingTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testWithNullPojoMap() {
        PojoDao<PojoMapEntity> dao = createPojoDao(PojoMapEntity.class);
        PojoMapEntity entity = new PojoMapEntity();
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertNull(entity.getPojos());
    }

    @Test
    public void testWithNullPrimitiveMap() {
        PojoDao<PrimitiveMapEntity> dao = createPojoDao(PrimitiveMapEntity.class);
        PrimitiveMapEntity entity = new PrimitiveMapEntity();
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertNull(entity.getInts());
    }

    @Test
    public void testWithPojoMap() {
        PojoDao<PojoMapEntity> dao = createPojoDao(PojoMapEntity.class);
        PojoMapEntity entity = new PojoMapEntity();
        Map<String,ElementEntity> expected = Maps.newHashMap();
        expected.put("1", new ElementEntity());
        expected.put("2", new ElementEntity());
        entity.setPojos(expected);
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertEquals(expected, entity.getPojos());
    }

    @Test
    public void testWithPrimitiveMap() {
        PojoDao<PrimitiveMapEntity> dao = createPojoDao(PrimitiveMapEntity.class);
        PrimitiveMapEntity entity = new PrimitiveMapEntity();
        Map<String,Integer> expected = Maps.newHashMap();
        expected.put("one", 1);
        expected.put("two", 2);
        entity.setInts(expected);
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertEquals(expected, entity.getInts());
    }

    @Test
    public void testWithNullKey() {
        assertHecateException("Cassandra driver does not support null key values inside Map(TEXT => INT, not frozen) collections.", () -> {
            PojoDao<PrimitiveMapEntity> dao = createPojoDao(PrimitiveMapEntity.class);
            PrimitiveMapEntity entity = new PrimitiveMapEntity();
            Map<String,Integer> expected = Maps.newHashMap();
            expected.put(null, 1);
            entity.setInts(expected);
            dao.save(entity);
        });
    }

    @Test
    public void testWithNullValue() {
        assertHecateException("Cassandra driver does not support null values inside Map(TEXT => INT, not frozen) collections.", () -> {
            PojoDao<PrimitiveMapEntity> dao = createPojoDao(PrimitiveMapEntity.class);
            PrimitiveMapEntity entity = new PrimitiveMapEntity();
            Map<String,Integer> expected = Maps.newHashMap();
            expected.put("one", null);
            entity.setInts(expected);
            dao.save(entity);
        });
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class PojoMapEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private Map<String,ElementEntity> pojos;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public Map<String, ElementEntity> getPojos() {
            return pojos;
        }

        public void setPojos(Map<String, ElementEntity> pojos) {
            this.pojos = pojos;
        }
    }

    public static class PrimitiveMapEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private Map<String,Integer> ints;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public Map<String, Integer> getInts() {
            return ints;
        }

        public void setInts(Map<String, Integer> ints) {
            this.ints = ints;
        }
    }
}