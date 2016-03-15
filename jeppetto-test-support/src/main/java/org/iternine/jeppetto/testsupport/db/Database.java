/*
 * Copyright (c) 2011-2014 Jeppetto and Jonathan Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.iternine.jeppetto.testsupport.db;


import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseSequenceFilter;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.statement.IBatchStatement;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.operation.DatabaseOperation;
import static org.dbunit.database.DatabaseConfig.PROPERTY_STATEMENT_FACTORY;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;


public abstract class Database {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private ConnectionSource connectionSource;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public Database(ConnectionSource connectionSource) {
        this.connectionSource = connectionSource;
    }


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    public abstract void close();


    protected abstract void onNewIDatabaseConnection(IDatabaseConnection connection);


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public void loadXmlDataSetResource(String resourceName) {
        loadXmlDataSet(Database.class.getResourceAsStream(resourceName));
    }


    public void loadXmlDataSet(InputStream inputStream) {
        IDatabaseConnection databaseConnection = null;

        try {
            databaseConnection = getIDatabaseConnection();

            IDataSet dataSet = new XmlDataSet(inputStream);

            DatabaseOperation.REFRESH.execute(databaseConnection, dataSet);
        } catch (DatabaseUnitException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (databaseConnection != null) {
                try { databaseConnection.close(); } catch (SQLException ignore) { }
            }
        }
    }


    public void writeXmlDataSet(OutputStream outputStream) {
        writeXmlDataSet(outputStream, null);
    }


    public void writeXmlDataSet(OutputStream outputStream, String[] tables) {
        IDatabaseConnection databaseConnection = null;

        try {
            databaseConnection = getIDatabaseConnection();

            ITableFilter sequenceFilter = new DatabaseSequenceFilter(databaseConnection);
            IDataSet dataSet = new FilteredDataSet(sequenceFilter, databaseConnection.createDataSet());

            if (tables != null) {
                dataSet = new FilteredDataSet(tables, dataSet);
            }

            XmlDataSet.write(dataSet, outputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (DataSetException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (databaseConnection != null) {
                try { databaseConnection.close(); } catch (SQLException ignore) { }
            }
        }
    }


    public void clearDatabase() {
        IDatabaseConnection databaseConnection = null;

        try {
            databaseConnection = getIDatabaseConnection();

            ITableFilter sequenceFilter = new DatabaseSequenceFilter(databaseConnection);
            IDataSet dataSet = new FilteredDataSet(sequenceFilter, databaseConnection.createDataSet());

            DatabaseOperation.DELETE_ALL.execute(databaseConnection, dataSet);
        } catch (DatabaseUnitException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (databaseConnection != null) {
                try { databaseConnection.close(); } catch (SQLException ignore) { }
            }
        }
    }


    public void executeArbitrarySql(String sql) {
        IDatabaseConnection databaseConnection = null;
        IBatchStatement statement = null;

        try {
            databaseConnection = getIDatabaseConnection();

            DatabaseConfig databaseConfig = databaseConnection.getConfig();
            IStatementFactory statementFactory
                    = (IStatementFactory) databaseConfig.getProperty(PROPERTY_STATEMENT_FACTORY);

            statement = statementFactory.createBatchStatement(databaseConnection);
            statement.addBatch(sql);
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null) {
                try { statement.close(); } catch (SQLException ignore) { }
            }

            if (databaseConnection != null) {
                try { databaseConnection.close(); } catch (SQLException ignore) { }
            }
        }
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected IDatabaseConnection getIDatabaseConnection()
            throws SQLException {
        IDatabaseConnection connection;

        try {
            connection = new DatabaseConnection(connectionSource.getConnection());
        } catch (DatabaseUnitException e) {
            throw new SQLException(e);
        }

        onNewIDatabaseConnection(connection);

        return connection;
    }
}
