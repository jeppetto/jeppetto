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

package org.iternine.jeppetto.dao.mongodb.enhance;


import java.util.List;
import java.util.Map;
import java.util.Set;


public class UpdateObjectHelper {

    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public boolean needsNoConversion(Class clazz) {
        return DBObjectUtil.needsNoConversion(clazz);
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
}
