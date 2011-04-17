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


import org.jeppetto.dao.NoSuchItemException;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.slf4j.Logger;


public final class QueryLoggingCommand extends MongoDBCommand {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private final MongoDBCommand delegate;
    private final Logger logger;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    private QueryLoggingCommand(MongoDBCommand delegate, Logger logger) {
        this.delegate = delegate;
        this.logger = logger;
    }


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    public static MongoDBCommand wrap(MongoDBCommand command, Logger logger) {
        return new QueryLoggingCommand(command, logger);
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    @Override
    public DBCursor cursor(DBCollection dbCollection) {
        logger.debug("Executing {} for {} cursor", delegate, dbCollection.getFullName());

        DBCursor dbCursor = delegate.cursor(dbCollection);

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            DBObject plan = dbCursor.explain();

            sb.append("MongoDB query plan ").append(plan).append('\n');
            sb.append("\tcursor = \"").append(plan.get("cursor")).append("\"\n");
            sb.append("\tnscanned = \"").append(plan.get("nscanned")).append("\"\n");
            sb.append("\tn = \"").append(plan.get("n")).append("\"\n");
            sb.append("\tmillis = \"").append(plan.get("millis")).append("\"\n");

            logger.debug(sb.toString());
        }

        return dbCursor;
    }

    
    @Override
    public Object singleResult(DBCollection dbCollection)
            throws NoSuchItemException {
        logger.debug("Executing {} for single {}", delegate, dbCollection.getName());

        return delegate.singleResult(dbCollection);
    }


    @Override
    public DBObject getQuery() {
        return delegate.getQuery();
    }
}
