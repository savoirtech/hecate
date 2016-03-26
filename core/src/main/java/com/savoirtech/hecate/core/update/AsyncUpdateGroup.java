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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.savoirtech.hecate.core.exception.HecateException;

public class AsyncUpdateGroup implements UpdateGroup {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final List<ResultSetFuture> futures = Collections.synchronizedList(Lists.newLinkedList());

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AsyncUpdateGroup(Session session) {
        this.session = session;
    }

//----------------------------------------------------------------------------------------------------------------------
// UpdateGroup Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void addUpdate(Statement statement) {
        futures.add(session.executeAsync(statement));
    }

    @Override
    public void complete() {
        try {
            Uninterruptibles.getUninterruptibly(Futures.allAsList(futures));
        } catch (ExecutionException e) {
            throw new HecateException("An exception occurred while awaiting statement completion.", e);
        }
    }

    @Override
    public ListenableFuture<Void> completeAsync() {
        return Futures.transform(Futures.allAsList(futures), (Function<List<ResultSet>, Void>) list -> null);
    }
}
