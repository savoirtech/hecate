/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.binding;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.core.schema.Table;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

public interface PojoBinding<P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Class<P> getPojoType();

    BoundStatement bindInsert(PreparedStatement statement, P pojo, int ttl);

    BoundStatement bindWhereIdEquals(PreparedStatement statement, List<Object> keys);

    P createPojo();

    void describe(Table table, Schema schema);

    Delete.Where deleteFrom(String tableName);

    KeyBinding getKeyBinding();

    void injectValues(P pojo, Row row, PojoQueryContext context);

    Insert insertInto(String tableName);

    Select.Where selectFrom(String tableName);

    Select.Where selectFromByKey(String tableName);

    void verifySchema(KeyspaceMetadata keyspaceMetadata, String tableName);

    Map<String,ParameterBinding> getParameterBindings();

    void visitChildren(P pojo, Predicate<Facet> predicate, PojoVisitor visitor);
}
