/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.connectors.jdbc.internal;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

@Experimental
public class RegionMapping implements Serializable {
  private final String regionName;
  private final String pdxClassName;
  private final String tableName;
  private final String connectionConfigName;
  private final Boolean primaryKeyInValue;
  private final ConcurrentMap<String, String> fieldToColumnMap;
  private final ConcurrentMap<String, String> columnToFieldMap;

  private final Map<String, String> configuredFieldToColumnMap;
  private final Map<String, String> configuredColumnToFieldMap;

  public RegionMapping(String regionName, String pdxClassName, String tableName,
      String connectionConfigName, Boolean primaryKeyInValue,
      Map<String, String> configuredFieldToColumnMap) {
    this.regionName = regionName;
    this.pdxClassName = pdxClassName;
    this.tableName = tableName;
    this.connectionConfigName = connectionConfigName;
    this.primaryKeyInValue = primaryKeyInValue;
    this.fieldToColumnMap = new ConcurrentHashMap<>();
    this.columnToFieldMap = new ConcurrentHashMap<>();
    if (configuredFieldToColumnMap != null) {
      this.configuredFieldToColumnMap =
          Collections.unmodifiableMap(new HashMap<>(configuredFieldToColumnMap));
      this.configuredColumnToFieldMap =
          Collections.unmodifiableMap(createReverseMap(configuredFieldToColumnMap));
    } else {
      this.configuredFieldToColumnMap = null;
      this.configuredColumnToFieldMap = null;
    }
  }

  private static Map<String, String> createReverseMap(Map<String, String> input) {
    Map<String, String> output = new HashMap<>();
    for (Map.Entry<String, String> entry : input.entrySet()) {
      String reverseMapKey = entry.getValue();
      String reverseMapValue = entry.getKey();
      if (output.containsKey(reverseMapKey)) {
        throw new IllegalArgumentException(
            "The field " + reverseMapValue + " can not be mapped to more than one column.");
      }
      output.put(reverseMapKey, reverseMapValue);
    }
    return output;
  }

  public String getConnectionConfigName() {
    return connectionConfigName;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getPdxClassName() {
    return pdxClassName;
  }

  public String getTableName() {
    return tableName;
  }

  public Boolean isPrimaryKeyInValue() {
    return primaryKeyInValue;
  }

  public String getRegionToTableName() {
    if (tableName == null) {
      return regionName;
    }
    return tableName;
  }

  private String getConfiguredColumnNameForField(String fieldName) {
    String result = fieldName;
    if (configuredFieldToColumnMap != null) {
      String mapResult = configuredFieldToColumnMap.get(fieldName);
      if (mapResult != null) {
        result = mapResult;
      }
    }
    return result;
  }

  public String getColumnNameForField(String fieldName, TableMetaDataView tableMetaDataView) {
    String columnName = fieldToColumnMap.get(fieldName);
    if (columnName == null) {
      String configuredColumnName = getConfiguredColumnNameForField(fieldName);
      Set<String> columnNames = tableMetaDataView.getColumnNames();
      if (columnNames.contains(configuredColumnName)) {
        // exact match
        columnName = configuredColumnName;
      } else {
        for (String candidate : columnNames) {
          if (candidate.equalsIgnoreCase(configuredColumnName)) {
            if (columnName != null) {
              throw new JdbcConnectorException(
                  "The SQL table has at least two columns that match the PDX field: " + fieldName);
            }
            columnName = candidate;
          }
        }
      }

      if (columnName == null) {
        columnName = configuredColumnName;
      }
      fieldToColumnMap.put(fieldName, columnName);
      columnToFieldMap.put(columnName, fieldName);
    }
    return columnName;
  }

  private String getConfiguredFieldNameForColumn(String columnName) {
    String result = columnName;
    if (configuredColumnToFieldMap != null) {
      String mapResult = configuredColumnToFieldMap.get(columnName);
      if (mapResult != null) {
        result = mapResult;
      }
    }
    return result;
  }

  public String getFieldNameForColumn(String columnName, TypeRegistry typeRegistry) {
    String fieldName = columnToFieldMap.get(columnName);
    if (fieldName == null) {
      String configuredFieldName = getConfiguredFieldNameForColumn(columnName);
      if (getPdxClassName() == null) {
        if (configuredFieldName.equals(configuredFieldName.toUpperCase())) {
          fieldName = configuredFieldName.toLowerCase();
        } else {
          fieldName = configuredFieldName;
        }
      } else {
        Set<PdxType> pdxTypes = getPdxTypesForClassName(typeRegistry);
        fieldName = findExactMatch(configuredFieldName, pdxTypes);
        if (fieldName == null) {
          fieldName = findCaseInsensitiveMatch(columnName, configuredFieldName, pdxTypes);
        }
      }
      assert fieldName != null;
      fieldToColumnMap.put(fieldName, columnName);
      columnToFieldMap.put(columnName, fieldName);
    }
    return fieldName;
  }

  private Set<PdxType> getPdxTypesForClassName(TypeRegistry typeRegistry) {
    Set<PdxType> pdxTypes = typeRegistry.getPdxTypesForClassName(getPdxClassName());
    if (pdxTypes.isEmpty()) {
      throw new JdbcConnectorException(
          "The class " + getPdxClassName() + " has not been pdx serialized.");
    }
    return pdxTypes;
  }

  /**
   * Given a column name and a set of pdx types, find the field name in those types that match,
   * ignoring case, the column name.
   *
   * @return the matching field name or null if no match
   * @throws JdbcConnectorException if no fields match
   * @throws JdbcConnectorException if more than one field matches
   */
  private String findCaseInsensitiveMatch(String columnName, String configuredFieldName,
      Set<PdxType> pdxTypes) {
    HashSet<String> matchingFieldNames = new HashSet<>();
    for (PdxType pdxType : pdxTypes) {
      for (String existingFieldName : pdxType.getFieldNames()) {
        if (existingFieldName.equalsIgnoreCase(configuredFieldName)) {
          matchingFieldNames.add(existingFieldName);
        }
      }
    }
    if (matchingFieldNames.isEmpty()) {
      throw new JdbcConnectorException("The class " + getPdxClassName()
          + " does not have a field that matches the column " + columnName);
    } else if (matchingFieldNames.size() > 1) {
      throw new JdbcConnectorException(
          "Could not determine what pdx field to use for the column name " + columnName
              + " because the pdx fields " + matchingFieldNames + " all match it.");
    }
    return matchingFieldNames.iterator().next();
  }

  /**
   * Given a column name, search the given pdxTypes for a field whose name exactly matches the
   * column name.
   *
   * @return the matching field name or null if no match
   */
  private String findExactMatch(String columnName, Set<PdxType> pdxTypes) {
    for (PdxType pdxType : pdxTypes) {
      if (pdxType.getPdxField(columnName) != null) {
        return columnName;
      }
    }
    return null;
  }

  public Map<String, String> getFieldToColumnMap() {
    return configuredFieldToColumnMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RegionMapping that = (RegionMapping) o;

    if (primaryKeyInValue != that.primaryKeyInValue) {
      return false;
    }
    if (regionName != null ? !regionName.equals(that.regionName) : that.regionName != null) {
      return false;
    }
    if (pdxClassName != null ? !pdxClassName.equals(that.pdxClassName)
        : that.pdxClassName != null) {
      return false;
    }
    if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) {
      return false;
    }
    if (connectionConfigName != null ? !connectionConfigName.equals(that.connectionConfigName)
        : that.connectionConfigName != null) {
      return false;
    }

    return (configuredFieldToColumnMap != null
        ? configuredFieldToColumnMap.equals(that.configuredFieldToColumnMap)
        : that.configuredFieldToColumnMap == null);
  }

  @Override
  public int hashCode() {
    int result = regionName != null ? regionName.hashCode() : 0;
    result = 31 * result + (pdxClassName != null ? pdxClassName.hashCode() : 0);
    result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
    result = 31 * result + (connectionConfigName != null ? connectionConfigName.hashCode() : 0);
    result = 31 * result + (primaryKeyInValue ? 1 : 0);
    result = 31 * result
        + (configuredFieldToColumnMap != null ? configuredFieldToColumnMap.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RegionMapping{" + "regionName='" + regionName + '\'' + ", pdxClassName='" + pdxClassName
        + '\'' + ", tableName='" + tableName + '\'' + ", connectionConfigName='"
        + connectionConfigName + '\'' + ", primaryKeyInValue=" + primaryKeyInValue
        + ", fieldToColumnMap=" + configuredFieldToColumnMap + '}';
  }
}
