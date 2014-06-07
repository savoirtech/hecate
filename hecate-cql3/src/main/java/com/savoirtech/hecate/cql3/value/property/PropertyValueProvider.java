package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.value.Value;
import com.savoirtech.hecate.cql3.value.ValueProvider;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class PropertyValueProvider implements ValueProvider {
//----------------------------------------------------------------------------------------------------------------------
// ValueProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public List<Value> getValues(Class<?> pojoType) {
        final PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(pojoType);
        final List<Value> values = new ArrayList<>(descriptors.length);
        for (PropertyDescriptor descriptor : descriptors) {
            final Method readMethod = ReflectionUtils.getReadMethod(pojoType, descriptor);
            final Method writeMethod = ReflectionUtils.getWriteMethod(pojoType, descriptor);
            if (readMethod != null && writeMethod != null) {
                values.add(new PropertyValue(descriptor, readMethod, writeMethod));
            }
        }
        return values;
    }
}
