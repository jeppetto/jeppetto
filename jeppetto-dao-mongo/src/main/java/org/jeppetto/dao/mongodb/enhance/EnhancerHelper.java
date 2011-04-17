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

package org.jeppetto.dao.mongodb.enhance;


import org.jeppetto.enhance.ChainingEnhancer;
import org.jeppetto.enhance.Enhancer;
import org.jeppetto.enhance.NoOpEnhancer;

import com.mongodb.DBObject;

import java.util.HashMap;
import java.util.Map;


public class EnhancerHelper {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private static Map<Class, Enhancer> dbObjectEnhancers = new HashMap<Class, Enhancer>();
    private static Map<Class, Enhancer> dirtyableEnhancers = new HashMap<Class, Enhancer>();
    private static Map<Class, Enhancer> dirtyableDBObjectEnhancers = new HashMap<Class, Enhancer>();


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    /**
     * Creates a new enhancer for the given class. If the class already implements
     * org.mongodb.DBObject, then a special "no-op" enhancer will be returned
     * that doesn't do any special enhancement. Otherwise, a byte-code enhancer is
     * returned.
     *
     * @param baseClass class for which to create an enhancer
     *
     * @return new enhancer
     */
    @SuppressWarnings( { "unchecked" })
    public static <T> Enhancer<T> getDBObjectEnhancer(Class<T> baseClass) {
        Enhancer<T> enhancer = (Enhancer<T>) dbObjectEnhancers.get(baseClass);

        if (enhancer == null) {
            if (DBObject.class.isAssignableFrom(baseClass)) {
                enhancer = new NoOpEnhancer<T>(baseClass);
            } else {
                enhancer = new DBObjectEnhancer<T>(baseClass);
            }

            dbObjectEnhancers.put(baseClass, enhancer);
        }

        return enhancer;
    }


    /**
     * Creates a new enhancer for the given class. If the class already implements
     * {@link Dirtyable}, then a special "no-op" enhancer will be returned that
     * doesn't do any special enhancement. Otherwise, a byte-code enhancer is returned.
     *
     * @param baseClass class for which to create an enhancer
     *
     * @return new enhancer
     */
    @SuppressWarnings( { "unchecked" })
    public static <T> Enhancer<T> getDirtyableEnhancer(Class<T> baseClass) {
        Enhancer<T> enhancer = (Enhancer<T>) dirtyableEnhancers.get(baseClass);

        if (enhancer == null) {
            if (Dirtyable.class.isAssignableFrom(baseClass)) {
                enhancer = new NoOpEnhancer<T>(baseClass);
            } else {
                enhancer = new DirtyableEnhancer<T>(baseClass);
            }

            dirtyableEnhancers.put(baseClass, enhancer);
        }

        return enhancer;
    }


    /**
     * Creates a ChainedEnhancer for the given class that allow it to be both a DBObject
     * and Dirtyable.
     *
     * @param baseClass class for which to create an enhancer
     *
     * @return new enhancer
     */
    @SuppressWarnings( { "unchecked" })
    public static <T> Enhancer<T> getDirtyableDBObjectEnhancer(Class<T> baseClass) {
        Enhancer<T> enhancer = (Enhancer<T>) dirtyableDBObjectEnhancers.get(baseClass);

        if (enhancer == null) {
            Enhancer<T> dbObjectEnhancer = getDBObjectEnhancer(baseClass);
            Enhancer<T> dirtyableEnhancer = getDirtyableEnhancer((Class<T>) dbObjectEnhancer.getEnhancedClass());

            enhancer = new ChainingEnhancer<T>(baseClass, dbObjectEnhancer, dirtyableEnhancer);

            dirtyableDBObjectEnhancers.put(baseClass, enhancer);
        }

        return enhancer;
    }
}
