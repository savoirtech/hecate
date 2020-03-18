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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

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

    Delete deleteFrom(String tableName);

    KeyBinding getKeyBinding();

    void injectValues(P pojo, Row row, PojoQueryContext context);

    Insert insertInto(String tableName);

    Select selectFrom(String tableName);

    Select selectFromByKey(String tableName);

    void verifySchema(KeyspaceMetadata keyspaceMetadata, String tableName);

    Map<String,ParameterBinding> getParameterBindings();

    void visitChildren(P pojo, Predicate<Facet> predicate, PojoVisitor visitor);
}
