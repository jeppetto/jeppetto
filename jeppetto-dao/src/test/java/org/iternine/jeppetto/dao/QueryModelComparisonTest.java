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
