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

package org.iternine.jeppetto.dao.updateobject;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


public abstract class UpdateObjectHelper {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Set<Class<?>> NO_CONVERSION_CLASSES = new HashSet<Class<?>>();


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    static {
        //noinspection unchecked
        Collections.addAll(NO_CONVERSION_CLASSES,
                           Date.class,
                           boolean.class,
                           char.class,
                           byte.class,
                           short.class,
                           int.class,
                           long.class,
                           float.class,
                           double.class,
                           Number.class,
                           BigDecimal.class,
                           BigInteger.class,
                           String.class,
                           Boolean.class,
                           Character.class,
                           Byte.class,
                           Short.class,
                           Integer.class,
                           Long.class,
                           Float.class,
                           Double.class,
                           Pattern.class,
                           byte[].class);
    }


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    public abstract Object getEnhancerMethod();


    public abstract String getListIndexFormat();


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public boolean needsUpdateObjectConversion(Class clazz) {
        return !NO_CONVERSION_CLASSES.contains(clazz);
    }


    public boolean isAssignableFromList(Class clazz) {
        return List.class.isAssignableFrom(clazz);
    }


    public boolean isAssignableFromMap(Class clazz) {
        return Map.class.isAssignableFrom(clazz);
    }


    public boolean isAssignableFromSet(Class clazz) {
        return Set.class.isAssignableFrom(clazz);
    }


    public String getAdderField(String methodName) {
        if (!methodName.startsWith("addTo")) {
            throw new RuntimeException("Unexpected adder: " + methodName);
        }

        String field = methodName.substring(5);

        return field.substring(0, 1).toLowerCase().concat(field.substring(1));
    }
}
