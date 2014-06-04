/*
 * Copyright 2014 Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.core.daotests;

import com.savoirtech.hecate.core.dao.ColumnFamilyDao;
import com.savoirtech.hecate.core.dao.PojoObjectGraphDao;
import com.savoirtech.hecate.core.test.CassandraTestCase;
import com.savoirtech.hecate.core.utils.DaoPool;
import me.prettyprint.hector.api.locking.HLock;
import org.junit.Test;


import static junit.framework.Assert.assertTrue;

public class PojoGraphDaoLockTest extends CassandraTestCase {

    @Test
    public void genericCollectionsToCF() {

        DaoPool<PojoObjectGraphDao> daoDaoPool = new DaoPool<>("cmp", keyspaceConfigurator, PojoObjectGraphDao.class);
        ColumnFamilyDao dao = daoDaoPool.getPojoDao(String.class, Top.class, "REDIRECT", null);

        Top top = new Top();
        top.setId("A");
        Child child = new Child();
        child.setId("A");

        top.getChildren().add(child);
        top.getMoreKids().add(child);

        dao.save(top.getId(), top);

        Top newTop = (Top) dao.find("A");

        assertTrue(newTop.getMoreKids().size() == 1);
    }

    @Test
    public void stringTest() {

        DaoPool<PojoObjectGraphDao> daoDaoPool = new DaoPool<>("cmp", keyspaceConfigurator, PojoObjectGraphDao.class);
        ColumnFamilyDao dao = daoDaoPool.getPojoDao(String.class, TopWithString.class, "REDIRECT", null);

        TopWithString top = new TopWithString();
        top.setId("A");
        Child child = new Child();
        child.setId("A");

        top.getChildren().add(child);
        top.getMoreKids().add(child);
        top.getStrings().add("A");
        HLock lock = hLockManager.createLock("/REDIRECT/" + top.getId());
        hLockManager.acquire(lock);

        dao.save(top.getId(), top);
        assertTrue(lock.isAcquired());
        hLockManager.release(lock);
        TopWithString newTop = (TopWithString) dao.find("A");

        assertTrue(newTop.getMoreKids().size() == 1);
        assertTrue(newTop.getStrings().size() == 1);
    }
}
