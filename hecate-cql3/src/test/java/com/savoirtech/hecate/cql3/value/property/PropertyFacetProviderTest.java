package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.annotations.Id;
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
    public void testGetGenericType() {
        Facet facet = getPropertyValue(PropertiesHolder.class, "mapProperty");
        assertNotNull(facet);
        final GenericType type = facet.getType();
        assertEquals(String.class, type.getTypeArgument(ReflectionUtils.MAP_KEY_TYPE_VAR, Map.class).getRawType());
        assertEquals(String.class, type.getTypeArgument(ReflectionUtils.MAP_VALUE_TYPE_VAR, Map.class).getRawType());
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
}