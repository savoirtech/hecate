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

package com.savoirtech.hecate.core.query;

import io.reactivex.Flowable;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public interface QueryResult<T> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Returns an iterator for the results
     *
     * @return an iterator for the results
     */
    Iterator<T> iterator();

    /**
     * Returns the results as a list
     *
     * @return the results as a list
     */
    List<T> list();

    /**
     * Returns a single result
     *
     * @return a single result
     */
    T one();

    /**
     * Returns a {@link Stream} of the results.
     *
     * @return a {@link Stream} of the results
     */
    Stream<T> stream();

    /**
     * Returns an RxJava {@link Flowable} of the results.
     * @return an RxJava {@link Flowable} of the results
     */
    Flowable<T> flowable();
}
