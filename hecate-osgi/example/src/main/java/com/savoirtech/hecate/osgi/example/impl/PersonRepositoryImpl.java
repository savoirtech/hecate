/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.osgi.example.impl;

import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;
import com.savoirtech.hecate.osgi.example.PersonRepository;
import com.savoirtech.hecate.osgi.example.model.Person;

public class PersonRepositoryImpl implements PersonRepository {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoDao<String, Person> dao;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PersonRepositoryImpl(PojoDaoFactory daoFactory) {
        this.dao = daoFactory.createPojoDao(Person.class);
    }

//----------------------------------------------------------------------------------------------------------------------
// PersonRepository Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Person findBySsn(String ssn) {
        return dao.findByKey(ssn);
    }

    @Override
    public void save(Person p) {
        dao.save(p);
    }
}
