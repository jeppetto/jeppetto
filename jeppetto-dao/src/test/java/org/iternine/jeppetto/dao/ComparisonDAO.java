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

package org.iternine.jeppetto.dao;


import org.iternine.jeppetto.dao.annotation.Association;
import org.iternine.jeppetto.dao.annotation.Condition;
import org.iternine.jeppetto.dao.annotation.DataAccessMethod;
import org.iternine.jeppetto.dao.annotation.Sort;

import java.util.List;


public interface ComparisonDAO extends GenericDAO<Sample, String> {

    // -----------------

    Sample findByFieldOne(int fieldOneValue);

    @DataAccessMethod(
        conditions = { @Condition(field = "fieldOne", type = ConditionType.Equal) }
    )
    Sample findUsingFieldOne(int fieldOneValue);


    // -----------------

    List<Sample> findByFieldOneGreaterThan(int fieldOneValue);

    @DataAccessMethod(
        conditions = { @Condition(field = "fieldOne", type = ConditionType.GreaterThan) }
    )
    List<Sample> findUsingFieldOneGreaterThan(int fieldOneValue);


    // -----------------

    Iterable<Sample> findByFieldOneLessThanOrderByFieldOneDesc(int fieldOneValue);

    @DataAccessMethod(
        conditions = { @Condition(field = "fieldOne", type = ConditionType.LessThan) },
        sorts = { @Sort(field = "fieldOne", direction = SortDirection.Descending) }
    )
    Iterable<Sample> findUsingFieldOneLessThanOrderByFieldOneDesc(int fieldOneValue);


    // -----------------

    List<Sample> findByHavingRelatedObjectWithRelatedIntValueLessThanOrderByIntValue(int relatedIntValueMax);

    @DataAccessMethod(
           associations = { @Association(field = "relatedObject",
                                         conditions = { @Condition(field = "relatedIntValue", type = ConditionType.LessThan)})},
           sorts = { @Sort(field = "intValue", direction = SortDirection.Ascending)}
    )
    List<Sample> findUsingHavingRelatedObjectWithRelatedIntValueLessThanOrderByIntValue(int relatedIntValueMax);


    // -----------------

    int countByHavingRelatedObjectsWithRelatedIntValueLessThan(int relatedIntValueMax);

    @DataAccessMethod(
           associations = { @Association(field = "relatedObjects",
                                         conditions = {@Condition(field = "relatedIntValue", type = ConditionType.LessThan)})},
           projections = { @org.iternine.jeppetto.dao.annotation.Projection(type = ProjectionType.RowCount) }
    )
    int countUsingHavingRelatedObjectsWithRelatedIntValueLessThan(int relatedIntValueMax);


    // -----------------

    List<Sample> findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimitAndSkip(int relatedIntValueMax, int limit, int skipCount);

    @DataAccessMethod(
            associations = { @Association(field = "relatedObjects",
                                          conditions = {@Condition(field = "relatedIntValue", type = ConditionType.LessThan)})},
            sorts = { @Sort(field = "intValue", direction = SortDirection.Ascending) },
            limitResults = true,
            skipResults = true
    )
    List<Sample> findUsingHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimitAndSkip(int relatedIntValueMax, int limit, int skipCount);


    // -----------------

    void deleteByIntValueWithin(List<Integer> someInts);


    @DataAccessMethod(
            operation = OperationType.Delete,
            conditions = { @Condition(field = "intValue", type = ConditionType.Within) }
    )
    void deleteUsingIntValueWithin(List<Integer> someInts);
}
