package com.savoirtech.hecate.cql3;

import org.junit.Test;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.Assert.*;

public class ReflectionUtilsTest {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGetWriteMethod() throws Exception {
        PropertyDescriptor descriptor = getPropertyDescriptor(Sub.class, "privateSetter");
        Method method = ReflectionUtils.getWriteMethod(Sub.class, descriptor);
        assertNotNull(method);
        assertEquals("setPrivateSetter", method.getName());
        assertTrue(Modifier.isPrivate(method.getModifiers()));
    }

    @Test
    public void testGetReadMethod() throws Exception {
        PropertyDescriptor descriptor = getPropertyDescriptor(Sub.class, "privateGetter");
        Method method = ReflectionUtils.getReadMethod(Sub.class, descriptor);
        assertNotNull(method);
        assertEquals("getPrivateGetter", method.getName());
        assertTrue(Modifier.isPrivate(method.getModifiers()));

    }

    private PropertyDescriptor getPropertyDescriptor(Class<?> beanType, String propertyName) throws Exception {
        BeanInfo beanInfo = Introspector.getBeanInfo(beanType);
        PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor descriptor : descriptors) {
            if (propertyName.equals(descriptor.getName())) {
                return descriptor;
            }
        }
        return null;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class Sub extends Super {
    }

    public static class Super {
        private String privateSetter;
        private String privateGetter;

        private String getPrivateGetter() {
            return privateGetter;
        }

        public void setPrivateGetter(String privateGetter) {
            this.privateGetter = privateGetter;
        }

        public String getPrivateSetter() {
            return privateSetter;
        }

        private void setPrivateSetter(String privateSetter) {
            this.privateSetter = privateSetter;
        }
    }
}