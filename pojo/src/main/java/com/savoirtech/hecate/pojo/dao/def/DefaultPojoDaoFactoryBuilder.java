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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.pojo.binding.PojoBindingFactory;
import com.savoirtech.hecate.pojo.binding.def.DefaultPojoBindingFactory;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterProvider;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.convert.def.ConstantConverterProvider;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryListener;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.naming.def.DefaultNamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.query.def.DefaultPojoQueryContextFactory;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;
import com.savoirtech.hecate.pojo.statement.def.DefaultPojoStatementFactory;

public class DefaultPojoDaoFactoryBuilder {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;

    private PojoBindingFactory bindingFactory;
    private NamingStrategy namingStrategy = new DefaultNamingStrategy();
    private PojoStatementFactory statementFactory;
    private PojoQueryContextFactory contextFactory;
    private FacetProvider facetProvider = new FieldFacetProvider();
    private ConverterRegistry converterRegistry;
    private int maximumCacheSize = DefaultPojoQueryContextFactory.DEFAULT_MAX_CACHE_SIZE;
    private Executor executor;
    private int threadPoolSize = DefaultPojoDaoFactory.DEFAULT_THREAD_POOL_SIZE;
    private List<PojoDaoFactoryListener> pojoFactoryListeners = new LinkedList<>();
    private List<ConverterProvider> converterProviders = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactoryBuilder(Session session) {
        this.session = session;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactory build() {
        if(executor == null) {
            executor = Executors.newFixedThreadPool(threadPoolSize, r -> new Thread(r, "Hecate"));
        }
        if(statementFactory == null) {
            statementFactory = new DefaultPojoStatementFactory(session);
        }
        if(contextFactory == null) {
            contextFactory = new DefaultPojoQueryContextFactory(session, statementFactory, maximumCacheSize);
        }
        if(converterRegistry == null) {
            DefaultConverterRegistry converterRegistry = new DefaultConverterRegistry();
            converterProviders.forEach(converterRegistry::registerConverter);
            this.converterRegistry = converterRegistry;
        }
        if(bindingFactory == null) {
            bindingFactory = new DefaultPojoBindingFactory(facetProvider, converterRegistry, namingStrategy);
        }
        DefaultPojoDaoFactory factory = new DefaultPojoDaoFactory(session, bindingFactory, statementFactory, contextFactory, namingStrategy, executor);
        pojoFactoryListeners.forEach(factory::addListener);
        return factory;
    }

    public DefaultPojoDaoFactoryBuilder withBindingFactory(PojoBindingFactory bindingFactory) {
        this.bindingFactory = bindingFactory;
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withContextFactory(PojoQueryContextFactory contextFactory) {
        this.contextFactory = contextFactory;
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withConverter(Converter converter) {
        converterProviders.add(new ConstantConverterProvider(converter));
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withConverter(ConverterProvider provider) {
        converterProviders.add(provider);
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withConverterRegistry(ConverterRegistry converterRegistry) {
        this.converterRegistry = converterRegistry;
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withExecutor(Executor executor) {
        this.executor = executor;
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withFacetProvider(FacetProvider facetProvider) {
        this.facetProvider = facetProvider;
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withListener(PojoDaoFactoryListener listener) {
        pojoFactoryListeners.add(listener);
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withMaximumCacheSize(int maximumCacheSize) {
        this.maximumCacheSize = maximumCacheSize;
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withNamingStrategy(NamingStrategy namingStrategy) {
        this.namingStrategy = namingStrategy;
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withStatementFactory(PojoStatementFactory statementFactory) {
        this.statementFactory = statementFactory;
        return this;
    }

    public DefaultPojoDaoFactoryBuilder withThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
        return this;
    }
}
