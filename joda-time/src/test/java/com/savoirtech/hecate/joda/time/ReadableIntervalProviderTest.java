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

package com.savoirtech.hecate.joda.time;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.MutableInterval;
import org.joda.time.ReadableInterval;
import org.junit.Test;

public class ReadableIntervalProviderTest extends JodaTimeConverterProviderTest {
    public ReadableIntervalProviderTest() {
        super(new ReadableIntervalProvider(), ReadableInterval.class);
    }

    @Test
    public void testSupportedTypes() {
        assertSupportsType(Interval.class, new Interval(DateTime.now(), DateTime.now().plus(1000)));
        assertSupportsType(MutableInterval.class, new MutableInterval(DateTime.now(), DateTime.now().plus(1000)));
    }
}
