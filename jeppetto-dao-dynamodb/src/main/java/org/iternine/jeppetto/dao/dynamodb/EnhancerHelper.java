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

package org.iternine.jeppetto.dao.dynamodb;


import org.iternine.jeppetto.dao.EntityVelocityEnhancer;
import org.iternine.jeppetto.dao.updateobject.UpdateObject;
import org.iternine.jeppetto.enhance.Enhancer;
import org.iternine.jeppetto.enhance.NoOpEnhancer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class EnhancerHelper {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private static final Map<Class, Enhancer> persistableEnhancers = new HashMap<Class, Enhancer>();
    private static final Map<Class, Enhancer> updateObjectEnhancers = new HashMap<Class, Enhancer>();


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    /**
     * Creates a new enhancer for the given class. If the class already implements
     * {@link DynamoDBPersistable}, then a special "no-op" enhancer will be returned that
     * doesn't do any special enhancement. Otherwise, a byte-code enhancer is returned.
     *
     * @param baseClass class for which to create an enhancer
     *
     * @return new enhancer
     */
    @SuppressWarnings( { "unchecked" })
    public static <T> Enhancer<T> getPersistableEnhancer(Class<T> baseClass) {
        if (persistableEnhancers.containsKey(baseClass)) {
            return (Enhancer<T>) persistableEnhancers.get(baseClass);
        }

        synchronized (persistableEnhancers) {
            Enhancer<T> enhancer = persistableEnhancers.get(baseClass);

            if (enhancer != null) {
                return enhancer;
            }

            if (DynamoDBPersistable.class.isAssignableFrom(baseClass)) {
                enhancer = new NoOpEnhancer<T>(baseClass);
            } else {
                enhancer = new EntityVelocityEnhancer<T>(baseClass) {
                    //-------------------------------------------------------------
                    // Implementation - Enhancer
                    //-------------------------------------------------------------

                    @Override
                    public boolean needsEnhancement(Object object) {
                        return object != null && !(object instanceof DynamoDBPersistable);
                    }


                    //-------------------------------------------------------------
                    // Implementation - VelocityEnhancer
                    //-------------------------------------------------------------

                    @Override
                    protected String getTemplateLocation() {
                        return "org/iternine/jeppetto/dao/dynamodb/enhance/dynamoDBPersistable.vm";
                    }
                };
            }

            persistableEnhancers.put(baseClass, enhancer);

            return enhancer;
        }
    }


    /**
     * Creates a new dynamoDBUpdateObject enhancer for the given class. If the class already implements
     * {@link UpdateObject}, then a special "no-op" enhancer will be returned that
     * doesn't do any special enhancement. Otherwise, a byte-code enhancer is returned.
     *
     * @param baseClass class for which to create an enhancer
     *
     * @return new enhancer
     */
    @SuppressWarnings( { "unchecked" })
    public static <T> Enhancer<T> getUpdateObjectEnhancer(Class<T> baseClass) {
        if (updateObjectEnhancers.containsKey(baseClass)) {
            return (Enhancer<T>) updateObjectEnhancers.get(baseClass);
        }

        synchronized (updateObjectEnhancers) {
            Enhancer<T> enhancer = updateObjectEnhancers.get(baseClass);

            if (enhancer != null) {
                return enhancer;
            }

            if (UpdateObject.class.isAssignableFrom(baseClass)) {
                enhancer = new NoOpEnhancer<T>(baseClass);
            } else {
                enhancer = new EntityVelocityEnhancer<T>(baseClass, Collections.singletonMap("updateObjectHelper",
                                                                                             (Object) new DynamoDBUpdateObjectHelper())) {
                    //-------------------------------------------------------------
                    // Implementation - Enhancer
                    //-------------------------------------------------------------

                    @Override
                    public boolean needsEnhancement(Object object) {
                        return object != null && !(object instanceof UpdateObject);
                    }


                    //-------------------------------------------------------------
                    // Implementation - VelocityEnhancer
                    //-------------------------------------------------------------

                    @Override
                    protected String getTemplateLocation() {
                        return "org/iternine/jeppetto/dao/enhance/updateObject.vm";
                    }
                };
            }

            updateObjectEnhancers.put(baseClass, enhancer);

            return enhancer;
        }
    }
}
