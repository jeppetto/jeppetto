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


import org.dbunit.database.IDatabaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MySQLDatabase extends Database {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final Logger logger = LoggerFactory.getLogger(MySQLDatabase.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public MySQLDatabase(ConnectionSource connectionSource) {
        super(connectionSource);
    }


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    public void close() {
        logger.warn("Cannot close MySQL database (likely not the correct semantic).  Instead, consider manually"
                    + " truncating or resetting any DB tables to a known state.");
    }


    protected void onNewIDatabaseConnection(IDatabaseConnection connection) {
        // Do nothing...
    }
}
