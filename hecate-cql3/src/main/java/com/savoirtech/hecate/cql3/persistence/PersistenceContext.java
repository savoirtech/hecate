/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.persistence;

import com.savoirtech.hecate.cql3.persistence.def.DefaultPojoQuery;

public interface PersistenceContext {

    PojoSave createSave(Class<?> pojoType, String tableName);

    <P> PojoQueryBuilder<P> find(Class<P> pojoType, String tableName);


    @SuppressWarnings("unchecked")
    <P> PojoQuery<P> findByKeys(Class<P> pojoType, String tableName);

    @SuppressWarnings("unchecked")
    <P> DefaultPojoQuery<P> findByKey(Class<P> pojoType, String tableName);
}
