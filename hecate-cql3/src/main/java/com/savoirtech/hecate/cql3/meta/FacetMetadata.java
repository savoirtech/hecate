package com.savoirtech.hecate.cql3.meta;

import com.savoirtech.hecate.cql3.annotations.ColumnName;
import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.annotations.TableName;
import com.savoirtech.hecate.cql3.util.GenericType;
import com.savoirtech.hecate.cql3.value.Facet;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class FacetMetadata {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Facet facet;
    private final String columnName;
    private final boolean identifier;
    private final String tableName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FacetMetadata(Facet facet) {
        this.facet = Validate.notNull(facet, "Facet cannot be null.");
        this.columnName = columnNameOf(facet);
        this.identifier = isIdentifier(facet);
        this.tableName = tableNameOf(facet);
    }

    private static String columnNameOf(Facet facet) {
        ColumnName annot = facet.getAnnotation(ColumnName.class);
        return annot == null ? facet.getName() : annot.value();
    }

    private static boolean isIdentifier(Facet facet) {
        Id annot = facet.getAnnotation(Id.class);
        return annot != null;
    }

    private static GenericType getElementType(Facet facet) {
        final GenericType facetType = facet.getType();
        final Class<?> facetRawType = facetType.getRawType();
        if (List.class.equals(facetRawType)) {
            return facetType.getListElementType();
        }
        if (Set.class.equals(facetRawType)) {
            return facetType.getSetElementType();
        }
        if (Map.class.equals(facetRawType)) {
            return facetType.getMapValueType();
        }
        if (facetRawType.isArray()) {
            return facetType.getArrayElementType();
        }
        return facetType;
    }

    private static String tableNameOf(Facet facet) {
        TableName annot = facet.getAnnotation(TableName.class);
        GenericType elementType = getElementType(facet);
        if (annot == null) {
            annot = elementType.getRawType().getAnnotation(TableName.class);
        }
        return annot == null ? elementType.getRawType().getSimpleName() : annot.value();
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public String getColumnName() {
        return columnName;
    }

    public Facet getFacet() {
        return facet;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isIdentifier() {
        return identifier;
    }
}
