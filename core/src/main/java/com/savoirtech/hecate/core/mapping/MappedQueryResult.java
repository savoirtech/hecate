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

package com.savoirtech.hecate.core.mapping;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Iterators;
import com.savoirtech.hecate.core.query.QueryResult;

public class MappedQueryResult<T> implements QueryResult<T> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Iterable<Row> rows;
    private final RowMapper<T> mapper;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------


    public MappedQueryResult(Iterable<Row> rows, RowMapper<T> mapper) {
        this.rows = rows;
        this.mapper = mapper;
    }

    public MappedQueryResult(ResultSet resultSet, RowMapper<T> mapper) {
        this.rows = resultSet;
        this.mapper = mapper;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Iterator<T> iterator() {
        Iterator<T> transformed = Iterators.transform(rows.iterator(), row -> {
            T value = mapper.map(row);
            mapper.mappingComplete();
            return value;
        });
        mapper.mappingComplete();
        return transformed;
    }

    @Override
    public List<T> list() {
        List<T> result = new LinkedList<>();
        Iterators.addAll(result, Iterators.transform(rows.iterator(), mapper::map));
        mapper.mappingComplete();
        return result;
    }

    @Override
    public T one() {
        Iterator<Row> iterator = rows.iterator();
        if(iterator.hasNext()) {
            T result = mapper.map(iterator.next());
            mapper.mappingComplete();
            return result;
        }
        return null;
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.IMMUTABLE), false);
    }
}
