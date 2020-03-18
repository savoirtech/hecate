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

import org.joda.time.*;
import org.junit.Test;

public class ReadablePeriodProviderTest extends JodaTimeConverterProviderTest {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ReadablePeriodProviderTest() {
        super(new ReadablePeriodProvider(), ReadablePeriod.class);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testSupportsPeriodTypes() {
        assertSupportsType(Days.class, Days.days(1));
        assertSupportsType(Hours.class, Hours.hours(2));
        assertSupportsType(Minutes.class, Minutes.minutes(3));
        assertSupportsType(Months.class, Months.months(4));
        assertSupportsType(MutablePeriod.class, new MutablePeriod(123L));
        assertSupportsType(Period.class, new Period(456L));
        assertSupportsType(Seconds.class, Seconds.seconds(5));
        assertSupportsType(Weeks.class, Weeks.weeks(6));
        assertSupportsType(Years.class, Years.years(7));
    }
}