package com.savoirtech.hecate.cql3.meta;


import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.annotations.TableName;
import com.savoirtech.hecate.cql3.annotations.Ttl;
import com.savoirtech.hecate.cql3.exception.HecateException;
import org.apache.commons.lang3.Validate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PojoMetadata {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<?> pojoType;
    private final String tableName;
    private final Integer timeToLive;
    private final Map<String, FacetMetadata> facets = new HashMap<>();
    private FacetMetadata identifierFacet;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    static String tableNameOf(Class<?> pojoType) {
        TableName annot = pojoType.getAnnotation(TableName.class);
        return annot == null ? pojoType.getSimpleName() : annot.value();
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoMetadata(Class<?> pojoType) {
        this.pojoType = Validate.notNull(pojoType, "POJO type cannot be null.");
        Validate.isTrue(ReflectionUtils.isInstantiable(pojoType), "Unable to instantiate POJOs of type %s", pojoType.getName());
        this.tableName = tableNameOf(pojoType);
        this.timeToLive = timeToLiveOf(pojoType);
    }

    private static Integer timeToLiveOf(Class<?> pojoType) {
        Ttl annot = pojoType.getAnnotation(Ttl.class);
        return annot == null ? null : annot.value();
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public FacetMetadata getIdentifierFacet() {
        return identifierFacet;
    }

    public Class<?> getPojoType() {
        return pojoType;
    }

    public String getDefaultTableName() {
        return tableName;
    }

    public Integer getTimeToLive() {
        return timeToLive;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PojoMetadata that = (PojoMetadata) o;

        if (!pojoType.equals(that.pojoType)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return pojoType.hashCode();
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void addFacet(FacetMetadata facetMetadata) {
        facets.put(facetMetadata.getColumnName(), facetMetadata);
        if (facetMetadata.isIdentifier()) {
            if (identifierFacet != null) {
                throw new HecateException(String.format("Duplicate identifiers found %s and %s.", facetMetadata.getColumnName(), identifierFacet.getColumnName()));
            }
            identifierFacet = facetMetadata;
        }
    }

    public Map<String, FacetMetadata> getFacets() {
        return Collections.unmodifiableMap(facets);
    }

    private Object newPojo() {
        return ReflectionUtils.instantiate(pojoType);
    }

    public Object newPojo(Object identifier) {
        Object pojo = newPojo();
        getIdentifierFacet().getFacet().set(pojo, identifier);
        return pojo;
    }

    public Map<Object, Object> newPojoMap(Iterable<Object> identifiers) {
        Map<Object, Object> pojos = new HashMap<>();
        for (Object identifier : identifiers) {
            pojos.put(identifier, newPojo(identifier));
        }
        return pojos;
    }
}
