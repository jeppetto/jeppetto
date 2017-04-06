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

package org.iternine.jeppetto.dao.persistable;


import java.util.Iterator;


public interface Persistable {

    /**
     * Consider the current state of the object to be in sync with the persisted state.
     */
    void __markPersisted(String storeIdentifier);


    /**
     * @return true if this object has a representation in the underlying store.
     */
    boolean __isPersisted(String storeIdentifier);


    /**
     * @return true if this object is dirtied from it's persisted state
     */
    boolean __isDirty();


    /**
     * @return Iterator corresponding to changed fields in this object
     */
    Iterator<String> __getDirtyFields();


    /**
     * @return The object this Persistable is delegating to, or null if it is managing everything itself
     */
    Object __getDelegate();
}
