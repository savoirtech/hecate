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

package com.savoirtech.hecate.pojo.dao;

import com.savoirtech.hecate.core.query.QueryResult;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.statement.StatementOptionsBuilder;
import com.savoirtech.hecate.core.update.UpdateGroup;
import com.savoirtech.hecate.pojo.persistence.PojoQueryBuilder;

public interface PojoDao<I, P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    default void delete(I id) {
        delete(id, StatementOptionsBuilder.empty());
    }

    void delete(I id, StatementOptions options);

    PojoQueryBuilder<P> find();

    P findById(I id);

    P findById(I id, StatementOptions options);

    default QueryResult<P> findByIds(Iterable<I> ids) {
        return findByIds(ids, StatementOptionsBuilder.empty());
    }

    QueryResult<P> findByIds(Iterable<I> ids, StatementOptions options);

    default void save(P pojo) {
        save(pojo, StatementOptionsBuilder.empty());
    }

    void save(P pojo, StatementOptions options);

    default void save(P pojo, int ttl) {
        save(pojo, ttl, StatementOptionsBuilder.empty());
    }

    void save(P pojo, int ttl, StatementOptions options);


    default void delete(UpdateGroup updateGroup, I id) {
        delete(updateGroup, id, StatementOptionsBuilder.empty());
    }

    void delete(UpdateGroup updateGroup, I id, StatementOptions options);

    default void save(UpdateGroup updateGroup, P pojo) {
        save(updateGroup, pojo, StatementOptionsBuilder.empty());
    }

    void save(UpdateGroup updateGroup, P pojo, StatementOptions options);

    default void save(UpdateGroup updateGroup, P pojo, int ttl) {
        save(updateGroup, pojo, ttl, StatementOptionsBuilder.empty());
    }

    void save(UpdateGroup updateGroup, P pojo, int ttl, StatementOptions options);
}
