/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jeppetto.testsupport.db;


import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.CompositeTable;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.FilteredTableMetaData;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.dataset.filter.DefaultTableFilter;
import org.dbunit.dataset.filter.IColumnFilter;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.dataset.xml.XmlDataSet;

import junit.framework.AssertionFailedError;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class DatabaseAssert {

    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    public static void assertDatabaseContentsEqualsResource(Database database, String resource) {
        assertDatabaseContentsEqualsResource(database, resource, new String[0]);
    }


    public static void assertDatabaseContentsEqualsResource(Database database, String resource, String[] ignoreList) {
        HashMap<String, String[]> ignoreMap = new HashMap<String, String[]>();

        ignoreMap.put("*", ignoreList);

        assertDatabaseContentsEqualsResource(database, resource, ignoreMap);
    }


    public static void assertDatabaseContentsEqualsResource(Database database, String resource,
                                                            Map<String, String[]> ignoreMap) {
        assertDatabaseContentsEquals(database, DatabaseAssert.class.getResourceAsStream(resource), ignoreMap);
    }


    public static void assertDatabaseContentsEquals(Database database, InputStream inputStream) {
        assertDatabaseContentsEquals(database, inputStream, new String[0]);
    }


    public static void assertDatabaseContentsEquals(Database database, InputStream inputStream, String[] ignoreList) {
        HashMap<String, String[]> ignoreMap = new HashMap<String, String[]>();

        ignoreMap.put("*", ignoreList);

        assertDatabaseContentsEquals(database, inputStream, ignoreMap);
    }


    public static void assertDatabaseContentsEquals(Database database, InputStream inputStream,
                                                    Map<String, String[]> ignoreMap) {
        Map<ITableFilter, IColumnFilter> filterMap = createFilterMap(ignoreMap);

        try {
            IDataSet databaseDataSet = database.getIDatabaseConnection().createDataSet();
            IDataSet actualDataSet = removeIgnoredColumns(databaseDataSet, filterMap);
            IDataSet xmlDataSet = new XmlDataSet(inputStream);
            IDataSet expectedDataSet = removeIgnoredColumns(xmlDataSet, filterMap);

            String[] expectedTableNames = expectedDataSet.getTableNames();

            for (String expectedTableName : expectedTableNames) {
                Assertion.assertEquals(new SortedTable(expectedDataSet.getTable(expectedTableName)),
                                       new SortedTable(actualDataSet.getTable(expectedTableName),
                                                       expectedDataSet.getTable(expectedTableName).getTableMetaData()));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (DataSetException e) {
            throw new RuntimeException(e);
        } catch (DatabaseUnitException e) {
            throw new AssertionFailedError(e.getMessage());
        }
    }


    //-------------------------------------------------------------
    // Methods - Private - Static
    //-------------------------------------------------------------

    private static Map<ITableFilter, IColumnFilter> createFilterMap(Map<String, String[]> ignoreMap) {
        HashMap<ITableFilter, IColumnFilter> filterMap = new HashMap<ITableFilter, IColumnFilter>();

        for (Map.Entry<String, String[]> e : ignoreMap.entrySet()) {
            DefaultTableFilter tables = new DefaultTableFilter();
            DefaultColumnFilter columnFilter = new DefaultColumnFilter();

            tables.includeTable(e.getKey());

            for (String columnPattern : e.getValue()) {
                columnFilter.excludeColumn(columnPattern);
            }

            filterMap.put(tables, columnFilter);
        }

        return filterMap;
    }


    private static IDataSet removeIgnoredColumns(IDataSet dataSet, Map<ITableFilter, IColumnFilter> columnFilterMap)
            throws DataSetException {
        DefaultDataSet filteredDataSet = new DefaultDataSet();

        ITableIterator iterator = dataSet.iterator();
        while (iterator.next()) {
            ITable table = iterator.getTable();

            for (Map.Entry<ITableFilter, IColumnFilter> e : columnFilterMap.entrySet()) {
                if (e.getKey().accept(table.getTableMetaData().getTableName())) {
                    table = filterTable(table, e.getValue());
                }
            }

            filteredDataSet.addTable(table);
        }

        return filteredDataSet;
    }


    private static ITable filterTable(ITable table, IColumnFilter filter)
            throws DataSetException {
        FilteredTableMetaData metaData = new FilteredTableMetaData(table.getTableMetaData(), filter);

        return new CompositeTable(metaData, table);
    }
}