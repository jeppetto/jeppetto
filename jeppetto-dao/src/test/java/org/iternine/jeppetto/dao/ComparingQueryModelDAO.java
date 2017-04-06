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


import java.util.Collections;
import java.util.Iterator;
import java.util.Map;


public class ComparingQueryModelDAO
        implements QueryModelDAO<Sample, String> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private QueryModel storedQueryModel;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected ComparingQueryModelDAO(Class<Sample> entityClass, Map<String, Object> daoProperties) {
        // ignore
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public Sample findById(String id)
            throws NoSuchItemException, JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public Iterable<Sample> findByIds(String... ids)
            throws JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public Iterable<Sample> findAll()
            throws JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public void save(Sample entity)
            throws OptimisticLockException, JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public void delete(Sample entity)
            throws JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public void deleteById(String id)
            throws JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public void deleteByIds(String... ids)
            throws JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public <U extends Sample> U getUpdateObject() {
        throw new UnsupportedOperationException();
    }


    @Override
    public <U extends Sample> Sample updateById(U updateObject, String s)
            throws JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public <U extends Sample> Iterable<Sample> updateByIds(U updateObject, String... strings)
            throws FailedBatchException, JeppettoException {
        throw new UnsupportedOperationException();
    }


    @Override
    public void flush()
            throws JeppettoException {
        throw new UnsupportedOperationException();
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    @Override
    public Sample findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException, TooManyItemsException, JeppettoException {
        compare(queryModel);

        return null;
    }


    @Override
    public Iterable<Sample> findUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        compare(queryModel);

        return Collections.emptyList();
    }


    @Override
    public Object projectUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        compare(queryModel);

        return 1;
    }


    @Override
    public void deleteUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        compare(queryModel);
    }


    @Override
    public <U extends Sample> Sample updateUniqueUsingQueryModel(U updateObject, QueryModel queryModel)
            throws JeppettoException {
        compare(queryModel);

        return null;
    }


    @Override
    public <U extends Sample> Iterable<Sample> updateUsingQueryModel(U updateObject, QueryModel queryModel)
            throws JeppettoException {
        compare(queryModel);

        return Collections.emptyList();
    }


    @Override
    public Condition buildCondition(String conditionField, ConditionType conditionType, Iterator argsIterator) {
        Condition condition = new Condition();

        condition.setField(conditionField);

        switch (conditionType) {
        case Equal:
        case NotEqual:
        case GreaterThan:
        case GreaterThanEqual:
        case LessThan:
        case LessThanEqual:
        case Within:
        case NotWithin:
            condition.setConstraint(conditionType + " " + argsIterator.next());

            break;

        case Between:
            condition.setConstraint(conditionType + " " + argsIterator.next() + " and " + argsIterator.next());

            break;

        case IsNull:
        case IsNotNull:
            condition.setConstraint(conditionType);

            break;

        default:
            throw new IllegalArgumentException("Unexpected enumeration: " + conditionType);
        }

        return condition;
    }


    @Override
    public Projection buildProjection(String projectionField, ProjectionType projectionType, Iterator argsIterator) {
        return new Projection(projectionField, projectionType.toString());
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void compare(QueryModel queryModel) {
        if (storedQueryModel == null) {
            storedQueryModel = queryModel;
        } else {
            try {
                if (!storedQueryModel.equals(queryModel)) {
                    throw new RuntimeException(String.format("QueryModels are not equal:\n%s\n%s", storedQueryModel, queryModel));
                }
            } finally {
                storedQueryModel = null;
            }
        }
    }
}
