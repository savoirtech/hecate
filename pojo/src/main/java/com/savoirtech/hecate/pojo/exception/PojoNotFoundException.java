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

package com.savoirtech.hecate.pojo.exception;

import java.util.List;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.PojoBinding;

import org.apache.commons.lang3.StringUtils;

public class PojoNotFoundException extends HecateException {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoNotFoundException(PojoBinding<?> binding, String tableName, List<Object> keys) {
        super("%s with key(s) %s not found in table \"%s\".", binding.getPojoType().getSimpleName(), StringUtils.join(keys, ","), tableName);
    }
}
