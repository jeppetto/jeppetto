/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
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
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class BasicDBObjectCommand extends MongoDBCommand {


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private BasicDBObject query;


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    public BasicDBObjectCommand(BasicDBObject query) {
        this.query = query;
    }


    //-------------------------------------------------------------
    // Override - MongoDBCommand
    //-------------------------------------------------------------

    @Override
    public DBCursor cursor(DBCollection dbCollection) {
        return dbCollection.find(query);
    }


    @SuppressWarnings({"unchecked"})
    @Override
    public Object singleResult(DBCollection dbCollection)
            throws NoSuchItemException {
        DBCursor cursor = cursor(dbCollection);
        cursor.limit(2);

        if (!cursor.hasNext()) {
            throw new NoSuchItemException(dbCollection.getName(), query.toString());
        }

        Object result = cursor.next();

        if (cursor.hasNext()) {
            throw new RuntimeException("More than one " + dbCollection.getName() + " matches query: " + query);
        }

        ((DirtyableDBObject) result).markPersisted();

        return result;
    }


    @Override
    public DBObject getQuery() {
        return query;
    }


    //-------------------------------------------------------------
    // Methods - Canonical
    //-------------------------------------------------------------

    @Override
    public String toString() {
        return super.toString() + ' ' + query;
    }
}
