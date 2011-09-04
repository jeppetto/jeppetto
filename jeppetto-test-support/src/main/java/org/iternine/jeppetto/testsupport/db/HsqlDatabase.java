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

package org.iternine.jeppetto.testsupport.db;


import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.datatype.DefaultDataTypeFactory;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.DatabaseConfig;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;


public class HsqlDatabase extends Database {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private static IDataTypeFactory hsqlDataTypeFactory = new DefaultDataTypeFactory() {
        public DataType createDataType(int sqlType, String sqlTypeName)
                throws DataTypeException {
             if (sqlType == Types.BOOLEAN) {
                 return DataType.BOOLEAN;
             }

             return super.createDataType(sqlType, sqlTypeName);
         }
    };


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public HsqlDatabase(ConnectionSource connectionSource) {
        super(connectionSource);
    }


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    public void close() {
        executeArbitrarySql("SHUTDOWN");
    }


    protected void onNewIDatabaseConnection(IDatabaseConnection connection) {
        DatabaseConfig config = connection.getConfig();

        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, hsqlDataTypeFactory);
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public void loadXmlDataSet(InputStream inputStream) {
        try {
            IDataSet dataSet = new XmlDataSet(inputStream);

            executeArbitrarySql("SET REFERENTIAL_INTEGRITY FALSE");

            DatabaseOperation.REFRESH.execute(getIDatabaseConnection(), dataSet);

            executeArbitrarySql("SET REFERENTIAL_INTEGRITY TRUE");
        } catch (DatabaseUnitException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void writeXmlDataSet(OutputStream outputStream, String[] tables) {
        try {
            IDataSet dataSet = getIDatabaseConnection().createDataSet();

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
        }
    }


    public void clearDatabase() {
        try {
            executeArbitrarySql("SET REFERENTIAL_INTEGRITY FALSE");

            IDatabaseConnection connection = getIDatabaseConnection();

            DatabaseOperation.DELETE_ALL.execute(connection, connection.createDataSet());

            executeArbitrarySql("SET REFERENTIAL_INTEGRITY TRUE");
        } catch (DatabaseUnitException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
