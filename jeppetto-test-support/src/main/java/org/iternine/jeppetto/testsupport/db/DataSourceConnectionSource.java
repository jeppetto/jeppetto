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


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;


public class DataSourceConnectionSource
        implements ConnectionSource {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private DataSource dataSource;
    private String driverClassName;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DataSourceConnectionSource(DataSource dataSource, String driverClassName) {
        this.dataSource = dataSource;
        this.driverClassName = driverClassName;
    }


    //-------------------------------------------------------------
    // Implementation - ConnectionSource
    //-------------------------------------------------------------

    public Connection getConnection()
            throws SQLException {
        return dataSource.getConnection();
    }


    public String getDriverClassName() {
        return driverClassName;
    }
}
