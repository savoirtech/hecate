package com.savoirtech.hecate.cql3.test;

import com.savoirtech.hecate.cql3.exception.HecateException;
import com.savoirtech.hecate.cql3.util.GenericType;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class GenericTypeTest {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGetRawTypeWithNonParameterizedType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("regular").getGenericType());
        assertEquals(String.class, type.getRawType());
    }

    @Test
    public void testGetRawTypeWithParameterizedType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("set").getGenericType());
        assertEquals(Set.class, type.getRawType());
    }

    @Test
    public void testGetSetElementType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("set").getGenericType());
        assertEquals(String.class, type.getSetElementType().getRawType());
    }

    @Test
    public void testGetListElementType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("list").getGenericType());
        assertEquals(String.class, type.getListElementType().getRawType());
    }

    @Test
    public void testGetMapKeyType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("map").getGenericType());
        assertEquals(String.class, type.getMapKeyType().getRawType());
    }

    @Test
    public void testGetMapValueType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("map").getGenericType());
        assertEquals(String.class, type.getMapValueType().getRawType());
    }

    @Test
    public void testGetNestedSetElementType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("setOfSets").getGenericType());
        assertEquals(Set.class, type.getSetElementType().getRawType());
        assertEquals(String.class, type.getSetElementType().getSetElementType().getRawType());
    }

    @Test(expected = HecateException.class)
    public void testGetTypeParameterWithInvalidArgument() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("set").getGenericType());
        assertEquals(String.class, type.getListElementType().getRawType());
    }

    @Test
    public void testGetArrayComponentType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("ints").getGenericType());
        assertEquals(int.class, type.getArrayElementType().getRawType());
    }

    @Test
    public void testGetMultidimensionalArrayComponentType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("ints2d").getGenericType());
        assertEquals(int[].class, type.getArrayElementType().getRawType());
    }

    @Test
    public void testGetGenericArrayElementType() throws Exception {
        GenericType type = new GenericType(Sub.class, Super.class.getField("array").getGenericType());
        assertEquals(String.class, type.getArrayElementType().getRawType());

    }


//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class Sub extends Super<String> {
    }

    public static class Super<T> {
        public T regular;
        public Set<T> set;
        public int[] ints;
        public int[][] ints2d;
        public T[] array;
        public List<T> list;
        public Map<T, T> map;
        public Set<Set<T>> setOfSets;
    }


}
