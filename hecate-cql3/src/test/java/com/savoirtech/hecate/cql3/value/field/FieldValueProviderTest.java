package com.savoirtech.hecate.cql3.value.field;

import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.value.Value;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FieldValueProviderTest {
    //----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------
    private FieldValue fooField() throws Exception {
        return new FieldValue(FieldHolder.class, FieldHolder.class.getDeclaredField("foo"));
    }

    @Test
    public void testGet() throws Exception {
        FieldValue value = fooField();
        final FieldHolder holder = new FieldHolder();
        assertEquals("bar", value.get(holder));
    }

    @Test
    public void testGetType() throws Exception {
        FieldValue value = fooField();
        assertEquals(String.class, value.getType().getRawType());
    }

    @Test
    public void testGetAnnotation() throws Exception {
        FieldValue value = fooField();
        assertNotNull(value.getAnnotation(Id.class));
    }


    @Test
    public void testGetName() throws Exception {
        FieldValue value = fooField();
        assertEquals("foo", value.getName());
    }

    @Test
    public void testSet() throws Exception {
        FieldValue value = fooField();
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
        @Id
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