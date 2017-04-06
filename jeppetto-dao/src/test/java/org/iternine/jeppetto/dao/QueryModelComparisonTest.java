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

package org.iternine.jeppetto.dao;


import org.junit.Test;

import java.util.Arrays;


public class QueryModelComparisonTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private ComparisonDAO comparisonDAO = DAOBuilder.buildDAO(Sample.class,
                                                              ComparisonDAO.class,
                                                              ComparingQueryModelDAO.class,
                                                              null);


    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Test
    public void findSingleImplicitEqualCondition() {
        comparisonDAO.findByFieldOne(10);
        comparisonDAO.findUsingFieldOne(10);
    }


    @Test
    public void findSingleExplicitCondition() {
        comparisonDAO.findByFieldOneGreaterThan(10);
        comparisonDAO.findUsingFieldOneGreaterThan(10);
    }


    @Test
    public void findSingleExplicitConditionWithSort() {
        comparisonDAO.findByFieldOneLessThanOrderByFieldOneDesc(10);
        comparisonDAO.findUsingFieldOneLessThanOrderByFieldOneDesc(10);
    }


    @Test
    public void findSingleAssociationExplicitConditionWithSort() {
        comparisonDAO.findByHavingRelatedObjectWithRelatedIntValueLessThanOrderByIntValue(10);
        comparisonDAO.findUsingHavingRelatedObjectWithRelatedIntValueLessThanOrderByIntValue(10);
    }


    @Test
    public void findSingleAssociationExplicitConditionWithSortAndLimitAndSkip() {
        comparisonDAO.findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimitAndSkip(0, 5, 10);
        comparisonDAO.findUsingHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimitAndSkip(0, 5, 10);
    }


    @Test
    public void countSingleAssociationExplicitCondition() {
        comparisonDAO.countByHavingRelatedObjectsWithRelatedIntValueLessThan(10);
        comparisonDAO.countUsingHavingRelatedObjectsWithRelatedIntValueLessThan(10);
    }


    @Test
    public void deleteSingleExplicitCondition() {
        comparisonDAO.deleteByIntValueWithin(Arrays.asList(1, 2, 3));
        comparisonDAO.deleteUsingIntValueWithin(Arrays.asList(1, 2, 3));
    }
}
