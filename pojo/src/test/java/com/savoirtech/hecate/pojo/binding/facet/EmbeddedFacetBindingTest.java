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

import java.io.Serializable;

import com.savoirtech.hecate.annotation.Embedded;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Test;

public class EmbeddedFacetBindingTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    @Cassandra
    public void testWithEmbedded() {
        PojoDao<Outer> dao = createPojoDao(Outer.class);
        Outer entity = new Outer();
        Inner inner = new Inner();
        inner.setA("foo");
        inner.setB("bar");
        entity.setInner(inner);
        dao.save(entity);

        Outer found = dao.findByKey(entity.getId());
        assertNotNull(found.getInner());
        assertEquals("foo", found.getInner().getA());
        assertEquals("bar", found.getInner().getB());
    }

    @Test
    @Cassandra
    public void testWithInvalidFieldInEmbedded() {
        assertHecateException("No converter found for facet \"inner.badField\" of type java.io.Serializable.", () -> createPojoDao(BadOuter.class));
    }

    @Test
    @Cassandra
    public void testWithNullEmbedded() {
        PojoDao<Outer> dao = createPojoDao(Outer.class);
        Outer entity = new Outer();
        dao.save(entity);

        Outer found = dao.findByKey(entity.getId());
        assertNull(found.getInner());
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class BadInner {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private Serializable badField;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public Serializable getBadField() {
            return badField;
        }

        public void setBadField(Serializable badField) {
            this.badField = badField;
        }
    }

    public static class BadOuter extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @Embedded
        private BadInner inner;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public BadInner getInner() {
            return inner;
        }

        public void setInner(BadInner inner) {
            this.inner = inner;
        }
    }

    public static class Inner {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private String a;
        private String b;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }

        public String getB() {
            return b;
        }

        public void setB(String b) {
            this.b = b;
        }
    }

    public static class Outer extends UuidEntity {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        @Embedded
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