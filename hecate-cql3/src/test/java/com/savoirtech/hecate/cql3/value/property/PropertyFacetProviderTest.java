/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.annotations.Transient;
import com.savoirtech.hecate.cql3.util.GenericType;
import com.savoirtech.hecate.cql3.value.Facet;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PropertyFacetProviderTest {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGetAnnotation() {
        Facet facet = getPropertyValue(PropertiesHolder.class, "normal");
        assertNotNull(facet.getAnnotation(Id.class));
    }

    private Facet getPropertyValue(Class<?> c, String propertyName) {
        PropertyFacetProvider provider = new PropertyFacetProvider();
        final List<Facet> facets = provider.getFacets(c);
        for (Facet facet : facets) {
            if (propertyName.equals(facet.getName())) {
                return facet;
            }
        }
        return null;
    }

    @Test
    public void testGetGenericType() {
        Facet facet = getPropertyValue(PropertiesHolder.class, "mapProperty");
        assertNotNull(facet);
        final GenericType type = facet.getType();
        assertEquals(String.class, type.getTypeArgument(ReflectionUtils.MAP_KEY_TYPE_VAR, Map.class).getRawType());
        assertEquals(String.class, type.getTypeArgument(ReflectionUtils.MAP_VALUE_TYPE_VAR, Map.class).getRawType());
    }

    @Test
    public void testGetType() {
        Facet facet = getPropertyValue(PropertiesHolder.class, "normal");
        assertEquals(String.class, facet.getType().getRawType());
    }

    @Test
    public void testGetValues() {
        PropertyFacetProvider provider = new PropertyFacetProvider();
        final List<Facet> facets = provider.getFacets(PropertiesHolder.class);
        assertEquals(4, facets.size());
    }

    @Test
    public void testInheritedProperties() {
        PropertyFacetProvider provider = new PropertyFacetProvider();
        final List<Facet> facets = provider.getFacets(Sub.class);
        assertEquals(1, facets.size());
        assertEquals("id", facets.get(0).getName());
    }

    @Test
    public void testWithPrivateGetter() {
        Facet facet = getPropertyValue(PropertiesHolder.class, "privateGetter");
        assertNotNull(facet);
        PropertiesHolder holder = new PropertiesHolder();
        holder.setPrivateGetter("foo");
        assertEquals("foo", facet.get(holder));
    }

    @Test
    public void testWithPrivateSetter() {
        Facet facet = getPropertyValue(PropertiesHolder.class, "privateSetter");
        assertNotNull(facet);
        PropertiesHolder holder = new PropertiesHolder();
        facet.set(holder, "foo");
        assertEquals("foo", holder.getPrivateSetter());
    }

    @Test
    public void testWithTransientAnnotation() {
        PropertyFacetProvider provider = new PropertyFacetProvider();
        final List<Facet> facets = provider.getFacets(TransientPropertyHolder.class);
        assertEquals(0, facets.size());
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class PropertiesHolder {
        private String normal;
        private String privateSetter;
        private String privateGetter;
        private Map<String, String> mapProperty;

        public Map<String, String> getMapProperty() {
            return mapProperty;
        }

        public void setMapProperty(Map<String, String> mapProperty) {
            this.mapProperty = mapProperty;
        }

        @Id
        public String getNormal() {
            return normal;
        }

        public void setNormal(String normal) {
            this.normal = normal;
        }

        public String getPrivateSetter() {
            return privateSetter;
        }

        private void setPrivateSetter(String privateSetter) {
            this.privateSetter = privateSetter;
        }

        private String getPrivateGetter() {
            return privateGetter;
        }

        public void setPrivateGetter(String privateGetter) {
            this.privateGetter = privateGetter;
        }
    }

    public static class TransientPropertyHolder {
        @Transient
        public String getTransientField() {
            return null;
        }

        public void setTransientField(String value) {
            // do nothing
        }
    }

    public static class Sub extends Super {
    }

    public static abstract class Super {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }
}