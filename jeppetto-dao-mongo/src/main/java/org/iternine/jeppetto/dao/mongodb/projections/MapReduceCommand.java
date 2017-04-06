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

package org.iternine.jeppetto.dao.mongodb.projections;


import org.iternine.jeppetto.dao.mongodb.MongoDBCommand;
import org.iternine.jeppetto.dao.mongodb.enhance.DBObjectUtil;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;

import java.util.Iterator;


/**
 * Base class for map reduce jobs.
 */
abstract class MapReduceCommand
        extends MongoDBCommand {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private DBObject query;


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    public MapReduceCommand(DBObject query) {
        this.query = query;
    }


    //-------------------------------------------------------------
    // Override - MongoDBCommand
    //-------------------------------------------------------------

    @Override
    public final Object singleResult(DBCollection dbCollection) {
        MapReduceOutput output = dbCollection.mapReduce(createMapFunction(), createReduceFunction(), null, query);
        Iterable<DBObject> results = output.results();

        try {
            return transformToValue(results);
        } finally {
            output.drop();
        }
    }


    @Override
    public String toString() {
        return super.toString() + ' ' + DBObjectUtil.toDBObject(query.toMap());
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected Object defaultValue() {
        return null;
    }


    protected Object transformToValue(Iterable<DBObject> results) {
        Iterator<DBObject> resultsIterator = results.iterator();

        if (resultsIterator.hasNext()) {
            return transformToValue(resultsIterator.next());
        } else {
            return defaultValue();
        }
    }


    //-------------------------------------------------------------
    // Methods - Protected - Abstract
    //-------------------------------------------------------------

    protected abstract String createMapFunction();


    protected abstract String createReduceFunction();


    protected abstract Object transformToValue(DBObject obj);
}
