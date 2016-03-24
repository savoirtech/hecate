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

package com.savoirtech.hecate.pojo.logging;

import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoLogging {
    public static final Logger CQL_LOGGER = LoggerFactory.getLogger("com.savoirtech.hecate.cql");

    public static void log(Class<?> pojoType, PreparedStatement statement, List<Object> params) {
        if (CQL_LOGGER.isDebugEnabled()) {
            CQL_LOGGER.debug("{}: {} parameters ({})", pojoType.getSimpleName(), statement.getQueryString(), StringUtils.join(params, ","));
        }
    }
}
