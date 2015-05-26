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

package com.savoirtech.hecate.pojo.persistence;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;


public interface PersistenceContext {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Dehydrator createDehydrator(int ttl, StatementOptions statementModifiers);

    Evaporator createEvaporator(StatementOptions options);

    Hydrator createHydrator(StatementOptions options);

    <P> PojoDelete delete(PojoMapping<P> mapping);

    ResultSet executeStatement(Statement statement, StatementOptions options);

    <P> PojoQueryBuilder<P> find(PojoMapping<P> mapping);

    <P> PojoQuery<P> findById(PojoMapping<P> mapping);

    <P> PojoFindByIds<P> findByIds(PojoMapping<P> mapping);

    <P> PojoFindForDelete findForDelete(PojoMapping<P> mapping);

    <P> PojoInsert<P> insert(PojoMapping<P> mapping);

    PreparedStatement prepare(RegularStatement statement);
}
