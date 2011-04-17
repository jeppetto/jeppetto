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


import org.jeppetto.dao.ConditionType;
import org.jeppetto.dao.GenericDAO;
import org.jeppetto.dao.NoSuchItemException;
import org.jeppetto.dao.ProjectionType;
import org.jeppetto.dao.SortDirection;
import org.jeppetto.dao.annotation.Association;
import org.jeppetto.dao.annotation.Condition;
import org.jeppetto.dao.annotation.DataAccessMethod;
import org.jeppetto.dao.annotation.Projection;
import org.jeppetto.dao.annotation.Sort;

import java.util.List;


public interface SimpleObjectDAO extends GenericDAO<SimpleObject, String> {

    SimpleObject findByIntValue(int intValue)
            throws NoSuchItemException;


    int countByIntValue(int intValue);


    int countByIntValueLessThan(int intValue);


    @DataAccessMethod( // findByIntValue()
            conditions = { @Condition(field = "intValue", type = ConditionType.Equal) }
    )
    SimpleObject findSimpleObject(int intValue)
            throws NoSuchItemException;


    @DataAccessMethod( // findByIntValueWithin()
            conditions = { @Condition(field = "intValue", type = ConditionType.Within) }
    )
    List<SimpleObject> findSomeObjects(List<Integer> someInts);


    @DataAccessMethod( // findByHavingRelatedObjectWithRelatedIntValueLessThanOrderByIntValue()
            associations = { @Association(field = "relatedObjects",
                                          conditions = { @Condition(field = "relatedIntValue", type = ConditionType.LessThan)})},
            sorts = { @Sort(field = "intValue", direction = SortDirection.Descending)}
    )
    List<SimpleObject> findAndSortRelatedItems(int relatedIntValueMax);


    @DataAccessMethod( // findByHavingRelatedObjectWithRelatedIntValueLessThanOrderByIntValue()
            associations = { @Association(field = "relatedObjects",
                                          conditions = { @Condition(field = "relatedIntValue", type = ConditionType.LessThan)})},
            sorts = { @Sort(field = "intValue", direction = SortDirection.Descending)}
    )
    Iterable<SimpleObject> findAndSortRelatedItemsIterable(int relatedIntValueMax);


    @DataAccessMethod( // findByHavingRelatedObjectWithRelatedIntValueLessThanOrderByIntValue()
            associations = { @Association(field = "relatedObjects",
                                          conditions = {@Condition(field = "relatedIntValue", type = ConditionType.LessThan)})},
            projections = { @Projection(type = ProjectionType.RowCount) }
    )
    int countRelatedItems(int relatedIntValueMax);


    @DataAccessMethod(
            associations = { @Association(field = "relatedObjects",
                                          conditions = { @Condition(field = "relatedIntValue", type = ConditionType.LessThan) })},
            sorts = { @Sort(field = "intValue", direction = SortDirection.Ascending) },
            limitResults = true
    )
    List<SimpleObject> limitRelatedItems(int relatedIntValueMax, int limit);


    @DataAccessMethod(
            associations = { @Association(field = "relatedObjects",
                                          conditions = {@Condition(field = "relatedIntValue", type = ConditionType.LessThan)})},
            sorts = { @Sort(field = "intValue", direction = SortDirection.Ascending) },
            limitResults = true,
            skipResults = true
    )
    List<SimpleObject> limitAndSkipRelatedItems(int relatedIntValueMax, int limit, int skipCount);


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
    double sumIntValues();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.Average, field = "intValue") }
    )
    double averageIntValues();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.Minimum, field = "intValue") }
    )
    double minIntValue();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.Maximum, field = "intValue") }
    )
    double maxIntValue();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.CountDistinct, field = "intValue") }
    )
    int countDistinctIntValue();


    @DataAccessMethod(
            projections = { @Projection(type = ProjectionType.CountDistinct, field = "intValue") }
    )
    int countIntValue();
}
