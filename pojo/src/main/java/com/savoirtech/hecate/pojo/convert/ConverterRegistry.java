/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.convert;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.type.GenericType;

import static com.savoirtech.hecate.core.exception.HecateException.verifyNotNull;

public interface ConverterRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Returns a {@link Converter} which can be used for the supplied value type
     *
     * @param valueType the value type
     * @return the converter or null if no converter was found
     */
    Converter getConverter(Class<?> valueType);

    /**
     * Returns a {@link Converter} which can be used for the supplied value type
     *
     * @param genericType the value type as a {@link GenericType}
     * @return the converter or null if no converter was found
     */
    default Converter getConverter(GenericType genericType) {
        return genericType == null ? null : getConverter(genericType.getRawType());
    }

    /**
     * Returns a {@link Converter} which can be used for the supplied value type.  If a converter cannot be found, a
     * {@link HecateException} is thrown.
     *
     * @param valueType the value type
     * @return the converter
     * @throws HecateException if no converter is found
     */
    default Converter getRequiredConverter(Class<?> valueType, String message, Object... params) {
        return verifyNotNull(getConverter(valueType), message, params);
    }

    /**
     * Returns a {@link Converter} which can be used for the supplied value type.  If a converter cannot be found, a
     * {@link HecateException} is thrown.
     *
     * @param genericType the value type as a {@link GenericType}
     * @return the converter
     * @throws HecateException if no converter is found
     */
    default Converter getRequiredConverter(GenericType genericType, String message, Object... params) {
        return getRequiredConverter(verifyNotNull(genericType, "GenericType parameter cannot be null.").getRawType(), message, params);
    }
}
