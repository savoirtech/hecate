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
import com.savoirtech.hecate.pojo.mapping.PojoMapping;

import java.util.List;
import java.util.function.Consumer;


public interface PersistenceContext {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Dehydrator createDehydrator();

    Hydrator createHydrator();

    Evaporator createEvaporator();

    ResultSet executeStatement(Statement statement, List<Consumer<Statement>> statementModifiers);
    
    <P> PojoInsert<P> insert(PojoMapping<P> mapping);

    <P> PojoDelete delete(PojoMapping<P> mapping);

    PreparedStatement prepare(RegularStatement statement);

    <P> PojoQueryBuilder<P> find(PojoMapping<P> mapping);

    <P> PojoQuery<P> findById(PojoMapping<P> mapping);

    <P> PojoQuery<P> findByIds(PojoMapping<P> mapping);
}
