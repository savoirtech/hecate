package com.savoirtech.hecate.cql3.value.field;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.annotations.IdColumn;
import com.savoirtech.hecate.cql3.value.Value;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FieldValueProviderTest {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGet() throws Exception {
        FieldValue value = new FieldValue(fooField());
        final FieldHolder holder = new FieldHolder();
        assertEquals("bar", value.get(holder));
    }

    @Test
    public void testGetType() throws Exception {
        FieldValue value = new FieldValue(fooField());
        assertEquals(String.class, value.getType());
    }

    private Field fooField() throws Exception {
        return FieldHolder.class.getDeclaredField("foo");
    }

    @Test
    public void testGetAnnotation() throws Exception {
        FieldValue value = new FieldValue(fooField());
        assertNotNull(value.getAnnotation(IdColumn.class));
    }

    @Test
    public void testGetGenericType() throws Exception {
        FieldValue value = new FieldValue(FieldHolder.class.getDeclaredField("map"));
        Type type = value.getGenericType();
        assertEquals(String.class, ReflectionUtils.mapKeyType(type));
        assertEquals(String.class, ReflectionUtils.mapValueType(type));
    }

    @Test
    public void testGetName() throws Exception {
        FieldValue value = new FieldValue(fooField());
        assertEquals("foo", value.getName());
    }

    @Test
    public void testSet() throws Exception {
        FieldValue value = new FieldValue(fooField());
        final FieldHolder holder = new FieldHolder();
        value.set(holder, "baz");
        assertEquals("baz", holder.foo);
    }

    @Test
    public void testWithNonPersistentFields() {
        FieldValueProvider provider = new FieldValueProvider();
        List<Value> values = provider.getValues(NonPersistentFields.class);
        assertEquals(0, values.size());
    }

    @Test
    public void testWithPersistentFields() {
        FieldValueProvider provider = new FieldValueProvider();
        final List<Value> values = provider.getValues(PersistentFields.class);
        assertEquals(1, values.size());
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class FieldHolder {
        @IdColumn
        private String foo = "bar";

        private Map<String, String> map;
    }

    public static class NonPersistentFields {
        private static String staticField = "static";
        private transient String transientField = "transient";
        private final String finalField = "final";
    }

    private static class PersistentFields {
        private String foo = "hello";
    }
}