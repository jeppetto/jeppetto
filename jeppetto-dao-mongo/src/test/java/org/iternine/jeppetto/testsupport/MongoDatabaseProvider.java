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

package org.iternine.jeppetto.testsupport;


import org.iternine.jeppetto.testsupport.db.Database;
import org.iternine.jeppetto.testsupport.db.MongoDatabase;

import org.springframework.context.ApplicationContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;


public class MongoDatabaseProvider
        implements DatabaseProvider, Closeable {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final String MONGODB_PORT_PROPERTY = "mongodb.left.port";
    private static final String BACKUP_MONGODB_PORT_PROPERTY = "mongodb.port";
    private static final String MONGODB_NAME_PROPERTY = "mongodb.dbname";


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private MongoDatabase db;
    private String mongoPortProperty;
    private String mongoDbNameProperty;
    private boolean uniquifyName;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    /**
     * Construct a default MongoDatabaseProvider that will uniquify mongodb database names.
     */
    public MongoDatabaseProvider() {
        this(true);
    }


    /**
     * Uses the default mongodb port properties (either "mongodb.left.port" or "mongodb.port").
     * Uses the default mongodb name property ("mongodb.dbname")
     *
     * @param uniquifyName true if the mongodb database name should be modified to avoid collisions with concurrent tests
     */
    public MongoDatabaseProvider(boolean uniquifyName) {
        this(MONGODB_PORT_PROPERTY, MONGODB_NAME_PROPERTY, uniquifyName);
    }


    /**
     * @param mongoPortProperty name of the property that specifies the mongodb port
     * @param mongoDbNameProperty name of the property that specifies the mongodb database name
     * @param uniquifyName true if the mongodb database name should be modified to avoid collisions with concurrent tests
     */
    public MongoDatabaseProvider(String mongoPortProperty, String mongoDbNameProperty, boolean uniquifyName) {
        this.mongoPortProperty = mongoPortProperty;
        this.mongoDbNameProperty = mongoDbNameProperty;
        this.uniquifyName = uniquifyName;
    }


    //-------------------------------------------------------------
    // Implementation - DatabaseProvider
    //-------------------------------------------------------------

    @Override
    public Properties modifyProperties(Properties properties) {
        int mongoDbPort;
        String mongoDbName;

        try {
            mongoDbPort = Integer.parseInt(properties.getProperty(mongoPortProperty));
        } catch (NumberFormatException nfe) {
            mongoDbPort = Integer.parseInt(properties.getProperty(BACKUP_MONGODB_PORT_PROPERTY));
        }

        if (uniquifyName) {
            String baseName = properties.getProperty(mongoDbNameProperty);

            mongoDbName = String.format("%s_%s", baseName,
                                        UUID.randomUUID().toString().substring(0, 3));

            properties.setProperty(mongoDbNameProperty, mongoDbName);
        } else {
            mongoDbName = properties.getProperty(mongoDbNameProperty);
        }

        // eager-initialization of db ahead of rest of spring config
        db = MongoDatabase.forPlatform(mongoDbPort);
        db.setMongoDbName(mongoDbName);

        return properties;
    }


    @Override
    public Database getDatabase(Properties properties, ApplicationContext applicationContext) {
        return db;
    }


    //-------------------------------------------------------------
    // Implementation - Closeable
    //-------------------------------------------------------------

    @Override
    public void close()
            throws IOException {
        if (db != null) {
            db.close();
        }
    }
}
