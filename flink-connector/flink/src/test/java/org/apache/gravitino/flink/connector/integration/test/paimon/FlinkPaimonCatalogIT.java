/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.flink.connector.integration.test.paimon;

import static org.apache.gravitino.flink.connector.integration.test.utils.TestUtils.toFlinkPhysicalColumn;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class FlinkPaimonCatalogIT extends FlinkCommonIT {

  protected org.apache.gravitino.Catalog catalog;

  @Override
  protected boolean supportSchemaOperationWithCommentAndOptions() {
    return false;
  }

  @Override
  protected String getProvider() {
    return "lakehouse-paimon";
  }

  @Override
  protected boolean supportDropCascade() {
    return true;
  }

  protected Catalog currentCatalog() {
    return catalog;
  }

  private void initPaimonCatalog() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    catalog =
        metalake.createCatalog(
            getPaimonCatalogName(),
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            getProvider(),
            null,
            getPaimonCatalogOptions());
  }

  protected abstract void createGravitinoCatalogByFlinkSql(String catalogName);

  protected abstract String getPaimonCatalogName();

  protected abstract Map<String, String> getPaimonCatalogOptions();

  @BeforeAll
  void paimonSetup() {
    initPaimonCatalog();
  }

  @AfterAll
  void paimonStop() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    metalake.dropCatalog(getPaimonCatalogName(), true);
  }

  protected abstract String getWarehouse();

  @Test
  public void testCreateGravitinoPaimonCatalogUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "gravitino_paimon_catalog";
    createGravitinoCatalogByFlinkSql(catalogName);
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(getWarehouse(), properties.get("warehouse"));
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs, tableEnv.listCatalogs().length, "The created catalog should be dropped.");
  }

  @Test
  public void testMultisetType() {
    String databaseName = "test_multiset_type_db";
    String tableName = "test_multiset_table";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          // Create a table with MULTISET column using ExternalType through Gravitino API.
          // This simulates the scenario where a Paimon table with MULTISET type is registered
          // in Gravitino (e.g., created by native Paimon and then managed by Gravitino).
          Column[] columns =
              new Column[] {
                Column.of("id", Types.LongType.get(), "id"),
                Column.of(
                    "field_multiset", Types.ExternalType.of("MULTISET<STRING>"), "multiset field")
              };
          catalog
              .asTableCatalog()
              .createTable(
                  NameIdentifier.of(databaseName, tableName),
                  columns,
                  "test multiset table",
                  ImmutableMap.of());

          // Verify that getTable through Gravitino Flink Connector returns MULTISET type
          Optional<org.apache.flink.table.catalog.Catalog> flinkCatalog =
              tableEnv.getCatalog(catalog.name());
          Assertions.assertTrue(flinkCatalog.isPresent());
          try {
            CatalogBaseTable table =
                flinkCatalog.get().getTable(new ObjectPath(databaseName, tableName));
            Assertions.assertNotNull(table);

            org.apache.flink.table.catalog.Column[] expected =
                new org.apache.flink.table.catalog.Column[] {
                  org.apache.flink.table.catalog.Column.physical("id", DataTypes.BIGINT())
                      .withComment("id"),
                  org.apache.flink.table.catalog.Column.physical(
                          "field_multiset", DataTypes.MULTISET(DataTypes.STRING()))
                      .withComment("multiset field")
                };
            org.apache.flink.table.catalog.Column[] actual =
                toFlinkPhysicalColumn(table.getUnresolvedSchema().getColumns());
            Assertions.assertArrayEquals(expected, actual);
          } catch (TableNotExistException e) {
            fail(e);
          }
        },
        true,
        supportDropCascade());
  }
}
