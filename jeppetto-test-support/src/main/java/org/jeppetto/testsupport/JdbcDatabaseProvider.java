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


import org.jeppetto.testsupport.db.ConnectionSource;
import org.jeppetto.testsupport.db.DataSourceConnectionSource;
import org.jeppetto.testsupport.db.Database;
import org.jeppetto.testsupport.db.DatabaseFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationContext;

import javax.sql.DataSource;

import java.util.Map;
import java.util.Properties;


public class JdbcDatabaseProvider
        implements DatabaseProvider {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Logger logger = LoggerFactory.getLogger(JdbcDatabaseProvider.class);


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String driverClassNameProperty;


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    public JdbcDatabaseProvider(String driverClassNameProperty) {
        this.driverClassNameProperty = driverClassNameProperty;
    }


    //-------------------------------------------------------------
    // Implementation - DatabaseProvider
    //-------------------------------------------------------------

    @Override
    public Properties modifyProperties(Properties properties) {
        return properties;
    }


    @Override
    public Database getDatabase(Properties properties, ApplicationContext applicationContext) {
        ConnectionSource connectionSource = this.getConnectionSource(properties, applicationContext);

        if (connectionSource != null) {
            return DatabaseFactory.getDatabase(connectionSource);
        } else {
            return null;
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private ConnectionSource getConnectionSource(Properties properties, ApplicationContext applicationContext) {
        Map datasourceBeans = applicationContext.getBeansOfType(DataSource.class);

        if (datasourceBeans.size() == 0) {
            return null;
        }

        if (datasourceBeans.size() > 1) {
            logger.warn("NOTE: Found more than one bean of type 'DataSource'.  Selecting random from the following: {}",
                        datasourceBeans);
        }

        DataSource dataSource = (DataSource) datasourceBeans.values().iterator().next();

        return new DataSourceConnectionSource(dataSource, (String) properties.get(driverClassNameProperty));
    }
}
