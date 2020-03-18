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

package com.savoirtech.hecate.pojo.dao.def;

import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryEvent;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryListener;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.CassandraSingleton;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DefaultPojoDaoFactoryTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    @Mock
    private PojoDaoFactoryListener listener;

    @Captor
    private ArgumentCaptor<PojoDaoFactoryEvent<ListenerEntity>> eventCaptor;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDaoFactoryListener() {
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactoryBuilder(CassandraSingleton.getSession()).withListener(listener).build();
        PojoDao<ListenerEntity> expected = factory.createPojoDao(ListenerEntity.class);
        verify(listener).pojoDaoCreated(eventCaptor.capture());
        PojoDaoFactoryEvent<ListenerEntity> event = eventCaptor.getValue();
        assertEquals("listener_entity", event.getTableName());
        assertEquals(expected, event.getPojoDao());
        assertEquals(ListenerEntity.class, event.getPojoBinding().getPojoType());

    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class ListenerEntity extends UuidEntity {
    }
}