/*
 * Copyright (c) 2011-2013 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.test.core;


import org.iternine.jeppetto.dao.ConditionType;
import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.annotation.Association;
import org.iternine.jeppetto.dao.annotation.Condition;
import org.iternine.jeppetto.dao.annotation.DataAccessMethod;
import org.iternine.jeppetto.dao.annotation.Projection;
import org.iternine.jeppetto.dao.test.SimpleObject;


public interface ProjectionDAO extends GenericDAO<SimpleObject, String> {

    int countByIntValue(int intValue);


    int countByIntValueLessThan(int intValue);


    @DataAccessMethod( // findByHavingRelatedObjectWithRelatedIntValueLessThanOrderByIntValue()
            associations = { @Association(field = "relatedObjects",
                                          conditions = {@Condition(field = "relatedIntValue", type = ConditionType.LessThan)})},
            projections = { @Projection(type = ProjectionType.RowCount) }
    )
    int countRelatedItems(int relatedIntValueMax);


    @DataAccessMethod( // countByIntValueGreaterThan
            conditions = { @Condition(field = "intValue", type = ConditionType.GreaterThan) },
            projections = { @Projection(type = ProjectionType.RowCount) }
    )
    int doAnAnnotationBasedCount(int intValue);


    @DataAccessMethod(
            conditions = { @Condition(field = "intValue", type = ConditionType.GreaterThanEqual) },
            projections = { @Projection(type = ProjectionType.RowCount) }
    )
    int doAnAnnotationBasedCountGreaterThanEquals(int intValue);


    @DataAccessMethod(
            conditions = { @Condition(field = "intValue", type = ConditionType.LessThanEqual) },
            projections = { @Projection(type = ProjectionType.RowCount) }
    )
    int doAnAnnotationBasedCountLessThanEquals(int intValue);


    // DSL-style
    int countByIntValueGreaterThanEqual(int intValue);


    // DSL-style
    int countByIntValueLessThanEqual(int intValue);


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.RowCount) }
    )
    int countAll();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.Sum, field = "intValue") }
    )
    int sumIntValues();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.Average, field = "intValue") }
    )
    double averageIntValues();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.Minimum, field = "intValue") }
    )
    int minIntValue();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.Maximum, field = "intValue") }
    )
    int maxIntValue();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.CountDistinct, field = "anotherIntValue") }
    )
    int countDistinctAnotherIntValue();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.CountDistinct, field = "intValue") }
    )
    int countIntValue();
}
