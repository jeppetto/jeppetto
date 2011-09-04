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

package org.iternine.jeppetto.dao.mongodb.projections;


import org.iternine.jeppetto.dao.Projection;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.mongodb.MongoDBCommand;

import com.mongodb.DBObject;


/**
 * Command factory.
 */
public final class ProjectionCommands {

    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    public static MongoDBCommand forProjection(Projection projection, DBObject query) {

        String field = projection.getField();

        switch ((ProjectionType) projection.getDetails()) {
            case Average:
                return new AverageMapReduceCommand(query, field);
            case Count:
                return new CountCommand(query, field);
            case CountDistinct:
                return new CountDistinctMapReduceCommand(query, field);
            case Maximum:
                return new MaximumMapReduceCommand(query, field);
            case Minimum:
                return new MinimumMapReduceCommand(query, field);
            case RowCount:
                return new RowCountCommand(query);
            case Sum:
                return new SumMapReduceCommand(query, field);
            default:
                throw new UnsupportedOperationException(projection.getDetails() + " not supported.");
        }
    }

    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    private ProjectionCommands() { /* empty utility constructor */ }

}
