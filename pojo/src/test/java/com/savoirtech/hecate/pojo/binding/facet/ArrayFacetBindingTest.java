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

import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Test;

public class ArrayFacetBindingTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    @Cassandra
    public void testWithNullPojoArray() {
        PojoDao<PojoArrayEntity> dao = createPojoDao(PojoArrayEntity.class);
        PojoArrayEntity entity = new PojoArrayEntity();
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertNull(entity.getPojos());
    }

    @Test
    @Cassandra
    public void testWithNullPrimitiveArray() {
        PojoDao<PrimitiveArrayEntity> dao = createPojoDao(PrimitiveArrayEntity.class);
        PrimitiveArrayEntity entity = new PrimitiveArrayEntity();
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertNull(entity.getInts());
    }

    @Test
    @Cassandra
    public void testWithPojoArray() {
        PojoDao<PojoArrayEntity> dao = createPojoDao(PojoArrayEntity.class);
        PojoArrayEntity entity = new PojoArrayEntity();
        ElementEntity[] expected = new ElementEntity[] {new ElementEntity(), new ElementEntity()};
        entity.setPojos(expected);
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertArrayEquals(expected, entity.getPojos());
    }

    @Test
    @Cassandra
    public void testWithPrimitiveArray() {
        PojoDao<PrimitiveArrayEntity> dao = createPojoDao(PrimitiveArrayEntity.class);
        PrimitiveArrayEntity entity = new PrimitiveArrayEntity();
        int[] expected = {3, 1, 4, 1, 5, 9};
        entity.setInts(expected);
        dao.save(entity);
        entity = dao.findByKey(entity.getId());
        assertArrayEquals(expected, entity.getInts());
    }

    @Test
    @Cassandra
    public void testWithNullElement() {
        assertHecateException("Cassandra driver does not support null values inside list<varchar> collections.", () -> {
            PojoDao<PojoArrayEntity> dao = createPojoDao(PojoArrayEntity.class);
            PojoArrayEntity entity = new PojoArrayEntity();
            entity.setPojos(new ElementEntity[] {new ElementEntity(), null, new ElementEntity()});
            dao.save(entity);
        });
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class PojoArrayEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private ElementEntity[] pojos;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public ElementEntity[] getPojos() {
            return pojos;
        }

        public void setPojos(ElementEntity[] pojos) {
            this.pojos = pojos;
        }
    }

    public static class PrimitiveArrayEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private int[] ints;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public int[] getInts() {
            return ints;
        }

        public void setInts(int[] ints) {
            this.ints = ints;
        }
    }
}