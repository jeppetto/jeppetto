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

package org.jeppetto.testsupport;


import org.jeppetto.testsupport.db.Database;
import org.jeppetto.testsupport.db.MongoDatabase;

import org.springframework.context.ApplicationContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;


public class MongoDatabaseProvider
        implements DatabaseProvider, Closeable {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private MongoDatabase db;
    private String mongoPortProperty = "mongodb.left.port";
    private String altMongoPortProperty = "mongodb.port";
    private String mongoDbNameProperty = "mongodb.dbname";


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public MongoDatabaseProvider() {
        this(null, null);
    }


    public MongoDatabaseProvider(String mongoPortProperty, String mongoDbNameProperty) {
        if (mongoPortProperty != null) {
            this.mongoPortProperty = mongoPortProperty;
        }

        if (mongoDbNameProperty != null) {
            this.mongoDbNameProperty = mongoDbNameProperty;
        }
    }


    //-------------------------------------------------------------
    // Implementation - DatabaseProvider
    //-------------------------------------------------------------

    @Override
    public Properties modifyProperties(Properties properties) {
        /*
        mongodb.host=127.0.0.1
        mongodb.port=27017
        mongodb.dbname=unittest
         */

        String baseName = properties.getProperty(mongoDbNameProperty);
        String uniqueName = String.format("%s_%s",
                                          UUID.randomUUID().toString().substring(0, 3),
                                          baseName);

        int port;
        try {
            port = Integer.parseInt(properties.getProperty(mongoPortProperty));
        } catch (NumberFormatException nfe) {
            port = Integer.parseInt(properties.getProperty(altMongoPortProperty));
        }

        properties.setProperty(mongoDbNameProperty, uniqueName);

        // eager-initialization of db ahead of rest of spring config
        db = MongoDatabase.forPlatform(port);
        db.setMongoDbName(uniqueName);

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
