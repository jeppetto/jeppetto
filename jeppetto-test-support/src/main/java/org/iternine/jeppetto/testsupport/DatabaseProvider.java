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

package org.iternine.jeppetto.testsupport;


import org.iternine.jeppetto.testsupport.db.Database;

import org.springframework.context.ApplicationContext;

import java.util.Properties;


/**
 * Interface implemented by classes that provide a database to a test context.
 */
public interface DatabaseProvider {

    /**
     * Optionally make any property modifications before refreshing the application context.
     *
     * @param properties properties
     * @return modified properties
     */
    Properties modifyProperties(Properties properties);


    /**
     * Create and initialize the database.
     *
     * @param properties app properties
     * @param applicationContext app context
     * @return new initialized database
     */
    Database getDatabase(Properties properties, ApplicationContext applicationContext);
}
