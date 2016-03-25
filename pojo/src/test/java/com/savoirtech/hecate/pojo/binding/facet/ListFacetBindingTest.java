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

import java.util.Arrays;
import java.util.List;

import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Test;

public class ListFacetBindingTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    @Cassandra
    public void testWithNullPojoList() {
        PojoDao<PojoListEntity> dao = createPojoDao(PojoListEntity.class);
        PojoListEntity entity = new PojoListEntity();
        dao.save(entity);
        entity = dao.findByKeys(entity.getId());
        assertNull(entity.getPojos());
    }

    @Test
    @Cassandra
    public void testWithNullPrimitiveList() {
        PojoDao<PrimitiveListEntity> dao = createPojoDao(PrimitiveListEntity.class);
        PrimitiveListEntity entity = new PrimitiveListEntity();
        dao.save(entity);
        entity = dao.findByKeys(entity.getId());
        assertNull(entity.getInts());
    }

    @Test
    @Cassandra
    public void testWithPojoList() {
        createTables(ElementEntity.class);
        PojoDao<PojoListEntity> dao = createPojoDao(PojoListEntity.class);
        PojoListEntity entity = new PojoListEntity();
        List<ElementEntity> expected = Arrays.asList(new ElementEntity(), new ElementEntity());
        entity.setPojos(expected);
        dao.save(entity);
        entity = dao.findByKeys(entity.getId());
        assertEquals(expected, entity.getPojos());
    }

    @Test
    @Cassandra
    public void testWithPrimitiveList() {
        PojoDao<PrimitiveListEntity> dao = createPojoDao(PrimitiveListEntity.class);
        PrimitiveListEntity entity = new PrimitiveListEntity();
        List<Integer> expected = Arrays.asList(3, 1, 4, 1, 5, 9);
        entity.setInts(expected);
        dao.save(entity);
        entity = dao.findByKeys(entity.getId());
        assertEquals(expected, entity.getInts());
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class PojoListEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private List<ElementEntity> pojos;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public List<ElementEntity> getPojos() {
            return pojos;
        }

        public void setPojos(List<ElementEntity> pojos) {
            this.pojos = pojos;
        }
    }

    public static class PrimitiveListEntity extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private List<Integer> ints;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public List<Integer> getInts() {
            return ints;
        }

        public void setInts(List<Integer> ints) {
            this.ints = ints;
        }
    }
}