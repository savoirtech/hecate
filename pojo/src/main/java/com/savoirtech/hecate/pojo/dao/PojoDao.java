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

import java.util.function.Consumer;

import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.update.UpdateGroup;
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.query.PojoQueryBuilder;

public interface PojoDao<P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    P findByKeys(Object... keys);

    P findByKeys(StatementOptions options, Object... keys);

    void delete(P pojo);

    void delete(StatementOptions options, P pojo);

    void delete(UpdateGroup group, StatementOptions options, P pojo);

    void deleteByKeys(Object... keys);

    void deleteByKeys(StatementOptions options, Object... keys);

    void deleteByKeys(UpdateGroup group, StatementOptions options, Object... keys);

    PojoQueryBuilder<P> find();

    PojoQuery<P> find(Consumer<Select.Where> builder);

    void save(P pojo);

    void save(P pojo, int ttl);

    void save(StatementOptions options, P pojo);

    void save(StatementOptions options, P pojo, int ttl);

    void save(UpdateGroup group, P pojo);

    void save(UpdateGroup group, P pojo, int ttl);

    void save(UpdateGroup group, StatementOptions options, P pojo);

    void save(UpdateGroup group, StatementOptions options, P pojo, int ttl);
}
