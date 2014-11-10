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

package org.iternine.jeppetto.dao.mongodb.enhance;


import org.iternine.jeppetto.dao.EntityVelocityEnhancer;
import org.iternine.jeppetto.enhance.Enhancer;
import org.iternine.jeppetto.enhance.NoOpEnhancer;

import java.util.HashMap;
import java.util.Map;


public class EnhancerHelper {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private static final Map<Class, Enhancer> dirtyableDBObjectEnhancers = new HashMap<Class, Enhancer>();
    private static final Map<Class, Enhancer> updateObjectEnhancers = new HashMap<Class, Enhancer>();


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    /**
     * Creates a new dirtyableDBObject Enhancer for the given class. If the class already implements
     * {@link DirtyableDBObject}, then a special "no-op" enhancer will be returned that
     * doesn't do any special enhancement. Otherwise, a byte-code enhancer is returned.
     *
     * @param baseClass class for which to create an enhancer
     *
     * @return new enhancer
     */
    @SuppressWarnings( { "unchecked" })
    public static <T> Enhancer<T> getDirtyableDBObjectEnhancer(Class<T> baseClass) {
        if (dirtyableDBObjectEnhancers.containsKey(baseClass)) {
            return (Enhancer<T>) dirtyableDBObjectEnhancers.get(baseClass);
        }

        synchronized (dirtyableDBObjectEnhancers) {
            Enhancer<T> enhancer;

            if (dirtyableDBObjectEnhancers.get(baseClass) != null) {
                enhancer = (Enhancer<T>) dirtyableDBObjectEnhancers.get(baseClass);
            } else if (DirtyableDBObject.class.isAssignableFrom(baseClass)) {
                enhancer = new NoOpEnhancer<T>(baseClass);

                dirtyableDBObjectEnhancers.put(baseClass, enhancer);
            } else {
                enhancer = new EntityVelocityEnhancer<T>(baseClass) {
                    //-------------------------------------------------------------
                    // Implementation - Enhancer
                    //-------------------------------------------------------------

                    @Override
                    public boolean needsEnhancement(Object object) {
                        return object != null && !(object instanceof DirtyableDBObject);
                    }


                    //-------------------------------------------------------------
                    // Implementation - VelocityEnhancer
                    //-------------------------------------------------------------

                    @Override
                    protected String getTemplateLocation() {
                        return "org/iternine/jeppetto/dao/mongodb/enhance/dirtyableDBObject.vm";
                    }
                };

                dirtyableDBObjectEnhancers.put(baseClass, enhancer);
            }

            return enhancer;
        }
    }


    /**
     * Creates a new dirtyableDBObject Enhancer for the given class. If the class already implements
     * {@link DirtyableDBObject}, then a special "no-op" enhancer will be returned that
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
            Enhancer<T> enhancer;

            if (updateObjectEnhancers.get(baseClass) != null) {
                enhancer = (Enhancer<T>) updateObjectEnhancers.get(baseClass);
            } else if (UpdateObject.class.isAssignableFrom(baseClass)) {
                enhancer = new NoOpEnhancer<T>(baseClass);

                updateObjectEnhancers.put(baseClass, enhancer);
            } else {
                Map<String, Object> contextItems = new HashMap<String, Object>();
                contextItems.put("updateObjectHelper", new UpdateObjectHelper());

                enhancer = new EntityVelocityEnhancer<T>(baseClass, contextItems) {
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
                        return "org/iternine/jeppetto/dao/mongodb/enhance/updateObject.vm";
                    }
                };

                updateObjectEnhancers.put(baseClass, enhancer);
            }

            return enhancer;
        }
    }
}
