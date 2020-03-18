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

package com.savoirtech.hecate.pojo.dao.listener;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.List;
import java.util.stream.Collectors;

import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.core.schema.SchemaItem;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryEvent;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSchemaListener implements PojoDaoFactoryListener {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSchemaListener.class);

    private final CqlSession session;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public CreateSchemaListener(CqlSession session) {
        this.session = session;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactoryListener Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <P> void pojoDaoCreated(PojoDaoFactoryEvent<P> event) {
        Schema schema = new Schema();
        event.getPojoBinding().describe(schema.createTable(event.getTableName()), schema);
        List<SimpleStatement> statements = schema.items().map(SchemaItem::createStatement).collect(Collectors.toList());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Creating table(s) to support \"{}\":\n\t{}\n", event.getPojoBinding().getPojoType().getSimpleName(), statements.stream().map(SimpleStatement::getQuery).collect(Collectors.joining("\n\t")));
        }
        statements.forEach(session::execute);
    }
}
