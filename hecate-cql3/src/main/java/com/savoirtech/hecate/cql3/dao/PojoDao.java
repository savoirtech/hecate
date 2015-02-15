/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.cql3.dao;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface PojoDao<K, P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void delete(K key);

    ListenableFuture<Void> deleteAsync(K key);

    P findByKey(K key);

    ListenableFuture<P> findByKeyAsync(K key);

    List<P> findByKeys(Iterable<K> keys);

    ListenableFuture<List<P>> findByKeysAsync(Iterable<K> keys);

    void save(P pojo);

    ListenableFuture<Void> saveAsync(P pojo);

    void save(P pojo, int ttl);

    ListenableFuture<Void> saveAsync(P pojo, int ttl);

}
