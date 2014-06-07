package com.savoirtech.hecate.cql3.value.field;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.value.Value;
import com.savoirtech.hecate.cql3.value.ValueProvider;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class FieldValueProvider implements ValueProvider {
//----------------------------------------------------------------------------------------------------------------------
// ValueProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public List<Value> getValues(Class<?> pojoType) {
        final List<Field> fields = ReflectionUtils.getFields(pojoType);
        final List<Value> values = new ArrayList<>();
        for (Field field : fields) {
            if (isPersistable(field)) {
                values.add(new FieldValue(field));
            }
        }
        return values;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private boolean isPersistable(Field field) {
        final int mods = field.getModifiers();
        return !(Modifier.isFinal(mods) ||
                Modifier.isTransient(mods) ||
                Modifier.isStatic(mods));
    }
}
