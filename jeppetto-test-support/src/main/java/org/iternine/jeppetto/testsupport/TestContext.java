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

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class TestContext {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private GenericApplicationContext applicationContext;
    private List<Database> databases = new ArrayList<Database>();


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public TestContext(String configurationFilename, String propertiesFilename) {

        this(configurationFilename, propertiesFilename, (String) null);
    }


    public TestContext(String configurationFilename, String propertiesFilename,
                       String driverClassNameProperty) {

        this(configurationFilename,
             propertiesFilename,
             (driverClassNameProperty == null) ? null : new DatabaseProvider[] { new JdbcDatabaseProvider(driverClassNameProperty) }
        );
    }


    public TestContext(String configurationFilename, String propertiesFilename, DatabaseProvider... databaseProviders) {
        XmlBeanFactory xmlBeanFactory = new XmlBeanFactory(new ClassPathResource(configurationFilename));
        xmlBeanFactory.setBeanClassLoader(this.getClass().getClassLoader());

        Properties properties = new Properties();

        try {
            properties.load(new ClassPathResource(propertiesFilename).getInputStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (databaseProviders != null) {
            for (DatabaseProvider databaseProvider : databaseProviders) {
                properties = databaseProvider.modifyProperties(properties);
            }
        }

        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setProperties(properties);
        configurer.postProcessBeanFactory(xmlBeanFactory);

        try {
            applicationContext = new GenericApplicationContext(xmlBeanFactory);
            applicationContext.refresh();

            if (databaseProviders != null) {
                for (DatabaseProvider databaseProvider : databaseProviders) {
                    databases.add(databaseProvider.getDatabase(properties, applicationContext));
                }
            }
        } catch (RuntimeException e) {
            if (databaseProviders != null) {
                for (DatabaseProvider databaseProvider : databaseProviders) {
                    if (databaseProvider instanceof Closeable) {
                        try {
                            ((Closeable) databaseProvider).close();
                        } catch (IOException e1) {
                            // ignore
                        }
                    }
                }
            }

            throw e;
        }
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public void close() {
        for (Database database : databases) {
            if (database != null) {
                database.close();
            }
        }

        applicationContext.close();
    }


    public Object getBean(String beanName) {
        return applicationContext.getBean(beanName);
    }


    public Database getDatabase() {
        return databases.get(0);
    }
}
