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

package com.savoirtech.hecate.core.update;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class BatchUpdateGroup implements UpdateGroup {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final BatchStatement batchStatement;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public BatchUpdateGroup(Session session) {
        this(session, BatchStatement.Type.LOGGED);
    }

    public BatchUpdateGroup(Session session, BatchStatement.Type batchType) {
        this.session = session;
        this.batchStatement = new BatchStatement(batchType);
    }

//----------------------------------------------------------------------------------------------------------------------
// UpdateGroup Implementation
//----------------------------------------------------------------------------------------------------------------------

    public void addUpdate(Statement statement) {
        batchStatement.add(statement);
    }

    @Override
    public void complete() {
        session.execute(batchStatement);
    }

    @Override
    public ListenableFuture<Void> completeAsync() {
        return Futures.transform(session.executeAsync(batchStatement), (Function<ResultSet, Void>) input -> null);
    }
}
