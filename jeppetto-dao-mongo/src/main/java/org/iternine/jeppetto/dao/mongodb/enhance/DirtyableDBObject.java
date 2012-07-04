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


import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.Iterator;


public interface DirtyableDBObject extends DBObject {

    /**
     * @return true if this object is dirtied from it's persisted state
     */
    boolean isDirty();


    /**
     * Consider the current state of the object to be in sync with the persisted state.
     */
    void markPersisted(DBCollection dbCollection);


    /**
     * @return true if this object has a representation in the underlying store.
     */
    boolean isPersisted(DBCollection dbCollection);

    
    /**
     * @return Set of keys corresponding to changed fields in this DBObject
     */
    Iterator<String> getDirtyKeys();


//    /**
//     *
//     */
//    void includeNullValuedKeys(boolean saveNulls);
}
