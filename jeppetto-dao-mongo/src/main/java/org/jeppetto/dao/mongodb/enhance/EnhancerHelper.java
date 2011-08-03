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


import org.jeppetto.enhance.Enhancer;
import org.jeppetto.enhance.NoOpEnhancer;
import org.jeppetto.enhance.VelocityEnhancer;

import java.util.HashMap;
import java.util.Map;


public class EnhancerHelper {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private static Map<Class, Enhancer> dirtyableDBObjectEnhancers = new HashMap<Class, Enhancer>();


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

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
    public static <T> Enhancer<T> getDirtyableDBObjectEnhancer(Class<T> baseClass) {
        Enhancer<T> enhancer = (Enhancer<T>) dirtyableDBObjectEnhancers.get(baseClass);

        if (enhancer == null) {
            if (Dirtyable.class.isAssignableFrom(baseClass)) {
                enhancer = new NoOpEnhancer<T>(baseClass);
            } else {
                enhancer = new VelocityEnhancer<T>(baseClass) {
                    //-------------------------------------------------------------
                    // Implementation - Enhancer
                    //-------------------------------------------------------------

                    @Override
                    public boolean needsEnhancement(Object object) {
                        return object != null && !(object instanceof Dirtyable);
                    }


                    //-------------------------------------------------------------
                    // Implementation - VelocityEnhancer
                    //-------------------------------------------------------------

                    @Override
                    protected String getTemplateLocation() {
                        return "org/jeppetto/dao/mongodb/enhance/dirtyableDBObject.vm";
                    }
                };
            }

            dirtyableDBObjectEnhancers.put(baseClass, enhancer);
        }

        return enhancer;
    }
}
