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

package com.savoirtech.hecate.pojo.mapping.verify;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMappingVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSchemaVerifier implements PojoMappingVerifier {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Logger LOGGER = LoggerFactory.getLogger(CreateSchemaVerifier.class);

    private final Session session;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public CreateSchemaVerifier(Session session) {
        this.session = session;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMappingVerifier Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void verify(PojoMapping<?> mapping) {
        Create create = mapping.createCreateStatement();
        LOGGER.info("Creating schema for {}...\n{}\n", mapping, create);
        session.execute(create);
    }
}
