/*
 * Copyright 2014 Savoir Technologies
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

package com.savoirtech.hecate.core.dao;

import java.util.List;
import java.util.Set;

public interface ColumnFamilyDao<K, T> {

    public void delete(K key);

    public Set<K> getKeys();

    public boolean containsKey(K key);

    public void save(K key, T pojo);

    public T find(K key);

    public Set<T> findItems(final List<K> keys, final String rangeFrom, final String rangeTo);
}


