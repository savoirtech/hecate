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

package com.savoirtech.hecate.pojo.cache;

import com.savoirtech.hecate.pojo.mapping.PojoMapping;

import java.util.Set;

public interface PojoCache {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    boolean contains(PojoMapping<? extends Object> mapping);

    Set<Object> idSet(PojoMapping<? extends Object> mapping);

    <P> P lookup(PojoMapping<P> mapping, Object id);
    <P> void put(PojoMapping<P> mapping, Object id, P pojo);

    <P> void putAll(PojoMapping<P> mapping, Iterable<P> pojos);
    
    long size(PojoMapping<? extends Object> mapping);
}
