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

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.core.schema.Table;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

public interface ColumnBinding {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void collectParameters(Object pojo, List<Object> parameters);

    void describe(Table table, Schema schema);

    List<ParameterBinding> getParameterBindings();

    void injectValues(Object pojo, Iterator<Object> columnValues, PojoQueryContext context);

    void insert(Insert insert);

    void select(Select.Selection select);

    void verifySchema(KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata);

    void visitChildren(Object pojo, Predicate<Facet> predicate, PojoVisitor visitor);
}
