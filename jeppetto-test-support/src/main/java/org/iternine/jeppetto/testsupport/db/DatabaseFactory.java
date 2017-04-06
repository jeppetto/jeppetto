/*
 * Copyright (c) 2011-2017 Jeppetto and Jonathan Thompson
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


public class DatabaseFactory {

    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    private DatabaseFactory() {
    }

    
    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public static Database getDatabase(ConnectionSource connectionSource) {
        String driverClassName = connectionSource.getDriverClassName();

        if (driverClassName.contains("hsql")) {
            return new HsqlDatabase(connectionSource);
        } else if (driverClassName.contains("mysql")) {
            return new MySQLDatabase(connectionSource);
        }

        throw new RuntimeException("Don't know what Database type to create for driver: " + driverClassName);
    }
}
