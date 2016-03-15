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

package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.TooManyItemsException;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


/**
 * Base MongoDB command.
 */
public abstract class MongoDBCommand {

    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public DBCursor cursor(DBCollection dbCollection) {
        throw new IllegalStateException(getClass().getSimpleName() + " does not provide a cursor.");
    }


    public Object singleResult(DBCollection dbCollection)
            throws NoSuchItemException, TooManyItemsException {
        throw new IllegalStateException(getClass().getSimpleName() + " does not provide a single result.");
    }


    public DBObject getQuery() {
        throw new IllegalStateException(getClass().getSimpleName() + " does not provide a query accessor.");
    }


    //-------------------------------------------------------------
    // Override - Object
    //-------------------------------------------------------------

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
