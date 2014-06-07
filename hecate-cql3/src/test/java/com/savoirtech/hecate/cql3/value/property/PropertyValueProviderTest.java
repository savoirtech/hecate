package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.annotations.IdColumn;
import com.savoirtech.hecate.cql3.value.Value;
import org.junit.Test;

import java.lang.reflect.Type;
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
        assertNotNull(value.getAnnotation(IdColumn.class));
    }

    @Test
    public void testGetType() {
        Value value = getPropertyValue(PropertiesHolder.class, "normal");
        assertEquals(String.class, value.getType());
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
        final Type type = value.getGenericType();
        assertEquals(String.class, ReflectionUtils.mapKeyType(type));
        assertEquals(String.class, ReflectionUtils.mapValueType(type));
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

        @IdColumn
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