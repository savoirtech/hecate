package com.savoirtech.hecate.cql3.value.field;

import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.value.Facet;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FieldFacetProviderTest {
    //----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------
    private FieldFacet fooField() throws Exception {
        return new FieldFacet(FieldHolder.class, FieldHolder.class.getDeclaredField("foo"));
    }

    @Test
    public void testGet() throws Exception {
        FieldFacet value = fooField();
        final FieldHolder holder = new FieldHolder();
        assertEquals("bar", value.get(holder));
    }

    @Test
    public void testGetType() throws Exception {
        FieldFacet value = fooField();
        assertEquals(String.class, value.getType().getRawType());
    }

    @Test
    public void testGetAnnotation() throws Exception {
        FieldFacet value = fooField();
        assertNotNull(value.getAnnotation(Id.class));
    }


    @Test
    public void testGetName() throws Exception {
        FieldFacet value = fooField();
        assertEquals("foo", value.getName());
    }

    @Test
    public void testSet() throws Exception {
        FieldFacet value = fooField();
        final FieldHolder holder = new FieldHolder();
        value.set(holder, "baz");
        assertEquals("baz", holder.foo);
    }

    @Test
    public void testWithNonPersistentFields() {
        FieldFacetProvider provider = new FieldFacetProvider();
        List<Facet> facets = provider.getFacets(NonPersistentFields.class);
        assertEquals(0, facets.size());
    }

    @Test
    public void testWithPersistentFields() {
        FieldFacetProvider provider = new FieldFacetProvider();
        final List<Facet> facets = provider.getFacets(PersistentFields.class);
        assertEquals(1, facets.size());
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