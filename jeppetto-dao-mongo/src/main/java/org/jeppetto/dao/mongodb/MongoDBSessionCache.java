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

package org.jeppetto.dao.mongodb;


import com.mongodb.DBObject;

import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


final class MongoDBSessionCache {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final Map<Map<String, String>, WeakReference> objectCache = new HashMap<Map<String, String>, WeakReference>();


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public void put(DBObject key, Object object) {
        //noinspection unchecked
        objectCache.put(flatten(key.toMap()), new WeakReference(object));
    }


    public Object get(DBObject key) {
        //noinspection unchecked
        Map<String, String> flattenedKey = flatten(key.toMap());
        WeakReference reference = objectCache.get(flattenedKey);

        if (reference == null) {
            return null;
        }

        if (reference.get() == null) {
            objectCache.remove(flattenedKey);

            return null;
        }
        
        return reference.get();
    }


    public void clear() {
        objectCache.clear();
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private Map<String, String> flatten(Map<String, ?> input) {
        Map<String, String> result = new HashMap<String, String>();

        for (Map.Entry<String, ?> entry : input.entrySet()) {
            for (Map.Entry<String, String> flattened : flatten(entry)) {
                if (flattened.getValue() != null) {
                    result.put(flattened.getKey(), flattened.getValue());
                }
            }
        }

        return result;
    }


    private Iterable<? extends Map.Entry<String, String>> flatten(Map.Entry<String, ?> entry) {
        if (entry.getValue() instanceof DBObject) {
            return flatten(entry.getKey(), ((DBObject) entry.getValue()).toMap());
        } else if (entry.getValue() != null) {
            return Collections.singletonList(
                    new AbstractMap.SimpleEntry<String, String>(entry.getKey(), entry.getValue().toString()));
        } else {
            return Collections.emptyList();
        }
    }


    private Iterable<? extends Map.Entry<String, String>> flatten(String key, Map<?, ?> value) {
        Map<String, String> result = new HashMap<String, String>();

        for (Map.Entry<?, ?> entry : value.entrySet()) {
            for (Map.Entry<String, String> flattened : flatten(new AbstractMap.SimpleEntry<String, Object>(key + '.' + entry.getKey(), entry.getValue()))) {
                result.put(flattened.getKey(), flattened.getValue());
            }
        }

        return result.entrySet();
    }
}
