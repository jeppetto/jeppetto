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

package org.iternine.jeppetto.dao.mongodb.projections;


import com.mongodb.DBObject;


/**
 * Map reduce job that counts distinct field values.
 */
class CountDistinctMapReduceCommand
        extends MapReduceCommand {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String field;


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    public CountDistinctMapReduceCommand(DBObject query, String field) {
        super(query);

        this.field = field;
    }


    //-------------------------------------------------------------
    // Override - MapReduceCommand
    //-------------------------------------------------------------

    @Override
    protected Integer defaultValue() {
        return 0;
    }


    @Override
    protected String createMapFunction() {
        return String.format("function() { emit( this.%1$s, 1 ); }", field);
    }


    @Override
    protected String createReduceFunction() {
        return "function(k,v) { " +
                "var sum=0; " +
                "for(var i in v) { " +
                "    sum += v[i]; " +
                "} " +
                "return sum; " +
                "}";
    }


    @Override
    protected Integer transformToValue(Iterable<DBObject> results) {
        int i = 0;

        for (DBObject dbObject : results) {
            i++;
        }

        return i;
    }


    @Override
    protected Integer transformToValue(DBObject dbObject) {
        throw new IllegalStateException("This method should not be invoked.");
    }
}
