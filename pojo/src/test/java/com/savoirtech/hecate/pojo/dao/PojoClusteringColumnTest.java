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

package com.savoirtech.hecate.pojo.dao;

import static org.junit.Assert.assertEquals;

import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.CassandraSingleton;
import org.junit.After;
import org.junit.Test;

public class PojoClusteringColumnTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    @After
    public void after() {
        CassandraSingleton.clean();
    }

    @Test
    public void testSave() {
        PojoDao<Outer> dao = createPojoDao(Outer.class);

        Outer outer = new Outer();
        outer.setInner(new Inner());
        dao.save(outer);

        Outer found = dao.findByKey(outer.getId(), outer.getInner().getId());
        assertEquals(outer, found);
        assertEquals(outer.getInner(), found.getInner());
    }

    public static class Inner extends UuidEntity {
    }

    public static class Outer extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @ClusteringColumn
        private Inner inner;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public Inner getInner() {
            return inner;
        }

        public void setInner(Inner inner) {
            this.inner = inner;
        }
    }
}
