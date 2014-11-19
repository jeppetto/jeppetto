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

package org.iternine.jeppetto.dao.updateobject;


import org.iternine.jeppetto.dao.JeppettoException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class UpdateMap
        implements UpdateObject, Map<String, Object> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Map<String, Object> updates = new HashMap<String, Object>();
    private boolean cleared = false;


    //-------------------------------------------------------------
    // Implementation - UpdateObject
    //-------------------------------------------------------------

    @Override
    public Map<String, Object> __getUpdates() {
        return updates;
    }


    //-------------------------------------------------------------
    // Implementation - Map
    //-------------------------------------------------------------

    @Override
    public Object put(String key, Object value) {
        return updates.put(key, value);
    }


    @Override
    public void putAll(Map<? extends String, ?> m) {
        for (Entry<? extends String, ?> entry : m.entrySet()) {
            updates.put(entry.getKey(), entry.getValue());
        }
    }


    @Override
    public Object remove(Object key) {
        return cleared ? updates.remove(key) : updates.put((String) key, null);
    }


    @Override
    public void clear() {
        cleared = true;
        updates.clear();
    }


    @Override
    public int size() {
        if (cleared) {
            return updates.size();
        }

        throw new JeppettoException("An un-cleared() UpdateMap does not support query operations.");
    }


    @Override
    public boolean isEmpty() {
        if (cleared) {
            return updates.isEmpty();
        }

        throw new JeppettoException("An un-cleared() UpdateMap does not support query operations.");
    }


    @Override
    public boolean containsKey(Object key) {
        if (cleared) {
            return updates.containsKey(key);
        }

        throw new JeppettoException("An un-cleared() UpdateMap does not support query operations.");
    }


    @Override
    public boolean containsValue(Object value) {
        if (cleared) {
            return updates.containsValue(value);
        }

        throw new JeppettoException("An un-cleared() UpdateMap does not support query operations.");
    }


    @Override
    public Object get(Object key) {
        if (cleared) {
            return updates.get(key);
        }

        throw new JeppettoException("An un-cleared() UpdateMap does not support query operations.");
    }


    @Override
    public Set<String> keySet() {
        if (cleared) {
            return updates.keySet();
        }

        throw new JeppettoException("An un-cleared() UpdateMap does not support query operations.");
    }


    @Override
    public Collection<Object> values() {
        if (cleared) {
            return updates.values();
        }

        throw new JeppettoException("An un-cleared() UpdateMap does not support query operations.");
    }


    @Override
    public Set<Entry<String, Object>> entrySet() {
        if (cleared) {
            return updates.entrySet();
        }

        throw new JeppettoException("An un-cleared() UpdateMap does not support query operations.");
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public boolean wasCleared() {
        return cleared;
    }
}