package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.util.GenericType;
import com.savoirtech.hecate.cql3.value.Value;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PropertyValueProviderTest {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGetAnnotation() {
        Value value = getPropertyValue(PropertiesHolder.class, "normal");
        assertNotNull(value.getAnnotation(Id.class));
    }

    @Test
    public void testGetType() {
        Value value = getPropertyValue(PropertiesHolder.class, "normal");
        assertEquals(String.class, value.getType().getRawType());
    }

    @Test
    public void testGetValues() {
        PropertyValueProvider provider = new PropertyValueProvider();
        final List<Value> values = provider.getValues(PropertiesHolder.class);
        assertEquals(4, values.size());
    }

    @Test
    public void testWithPrivateGetter() {
        Value value = getPropertyValue(PropertiesHolder.class, "privateGetter");
        assertNotNull(value);
        PropertiesHolder holder = new PropertiesHolder();
        holder.setPrivateGetter("foo");
        assertEquals("foo", value.get(holder));
    }

    @Test
    public void testWithPrivateSetter() {
        Value value = getPropertyValue(PropertiesHolder.class, "privateSetter");
        assertNotNull(value);
        PropertiesHolder holder = new PropertiesHolder();
        value.set(holder, "foo");
        assertEquals("foo", holder.getPrivateSetter());
    }

    @Test
    public void testGetGenericType() {
        Value value = getPropertyValue(PropertiesHolder.class, "mapProperty");
        assertNotNull(value);
        final GenericType type = value.getType();
        assertEquals(String.class, type.getTypeArgument(ReflectionUtils.MAP_KEY_TYPE_VAR, Map.class).getRawType());
        assertEquals(String.class, type.getTypeArgument(ReflectionUtils.MAP_VALUE_TYPE_VAR, Map.class).getRawType());
    }

    private Value getPropertyValue(Class<?> c, String propertyName) {
        PropertyValueProvider provider = new PropertyValueProvider();
        final List<Value> values = provider.getValues(c);
        for (Value value : values) {
            if (propertyName.equals(value.getName())) {
                return value;
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