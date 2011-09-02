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

package org.jeppetto.dao.hibernate;


import org.jeppetto.dao.Condition;
import org.jeppetto.dao.ConditionType;
import org.jeppetto.dao.NoSuchItemException;
import org.jeppetto.dao.Projection;
import org.jeppetto.dao.ProjectionType;
import org.jeppetto.dao.QueryModel;
import org.jeppetto.dao.QueryModelDAO;
import org.jeppetto.dao.Sort;
import org.jeppetto.dao.SortDirection;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * An implementation of the QueryModelDAO interface that supports Hibernate.
 *
 * @param <T> persistent class.
 * @param <ID> ID type for the persistent class.
 */
public class HibernateQueryModelDAO<T, ID extends Serializable>
        implements QueryModelDAO<T, ID> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Class<T> persistentClass;
    private SessionFactory sessionFactory;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public HibernateQueryModelDAO(Class<T> persistentClass, Map<String, Object> daoProperties) {
        this.persistentClass = persistentClass;
        this.sessionFactory = (SessionFactory) daoProperties.get("sessionFactory");
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(ID id)
            throws NoSuchItemException {
        T entity = persistentClass.cast(getCurrentSession().get(persistentClass, id));

        if (entity == null) {
            throw new NoSuchItemException(persistentClass.getSimpleName(), id.toString());
        }

        return entity;
    }


    @Override
    public Iterable<T> findAll() {
        return findUsingQueryModel(new QueryModel());
    }


    @Override
    public void save(T entity) {
        try {
            getCurrentSession().saveOrUpdate(entity);
        } catch (HibernateException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void delete(T entity) {
        try {
            getCurrentSession().delete(entity);
        } catch (HibernateException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void deleteById(ID id) {
        try {
            getCurrentSession().delete(findById(id));
        } catch (NoSuchItemException ignore) {
            // If it doesn't exist, no need to delete.
        } catch (HibernateException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void flush() {
        getCurrentSession().flush();
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    @Override
    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException {
        Criteria criteria = buildCriteria(queryModel);

        // keep suppressed because if the return type is 'int', then the call to Class.cast will fail
        // noinspection unchecked
        T result = (T) criteria.uniqueResult();

        if (result == null) {
            throw new NoSuchItemException(persistentClass.getSimpleName(), queryModel.toString());
        }

        return result;
    }


    @Override
    public Iterable<T> findUsingQueryModel(QueryModel queryModel) {
        Criteria criteria = buildCriteria(queryModel);

        if (queryModel.getSorts() != null) {
            for (Sort sort : queryModel.getSorts()) {
                criteria.addOrder(sort.getSortDirection() == SortDirection.Ascending ? Order.asc(sort.getField())
                                                                                     : Order.desc(sort.getField()));
            }
        }

        if (queryModel.getMaxResults() > 0) {
            criteria.setMaxResults(queryModel.getMaxResults());
        }

        if (queryModel.getFirstResult() > 0) {
            criteria.setFirstResult(queryModel.getFirstResult());
        }

        //noinspection unchecked
        return criteria.list();
    }


    @Override
    public Object projectUsingQueryModel(QueryModel queryModel) {
        return buildCriteria(queryModel).uniqueResult();
    }


    @Override
    public Condition buildCondition(String conditionField,
                                    ConditionType conditionType,
                                    Iterator argsIterator) {
        Condition condition = new Condition();

        condition.setField(conditionField);

        switch (conditionType) {
        case Between:
            condition.setConstraint(Restrictions.between(conditionField, argsIterator.next(), argsIterator.next()));
            break;

        case Equal:
            condition.setConstraint(Restrictions.eq(conditionField, argsIterator.next()));
            break;

        case GreaterThan:
            condition.setConstraint(Restrictions.gt(conditionField, argsIterator.next()));
            break;

        case GreaterThanEqual:
            condition.setConstraint(Restrictions.ge(conditionField, argsIterator.next()));
            break;

        case IsNotNull:
            condition.setConstraint(Restrictions.isNotNull(conditionField));
            break;

        case IsNull:
            condition.setConstraint(Restrictions.isNull(conditionField));
            break;

        case LessThan:
            condition.setConstraint(Restrictions.lt(conditionField, argsIterator.next()));
            break;

        case LessThanEqual:
            condition.setConstraint(Restrictions.le(conditionField, argsIterator.next()));
            break;

        case NotEqual:
            condition.setConstraint(Restrictions.ne(conditionField, argsIterator.next()));
            break;

        case NotWithin:
            condition.setConstraint(Restrictions.not(Restrictions.in(conditionField, (Collection) argsIterator.next())));
            break;

        case Within:
            condition.setConstraint(Restrictions.in(conditionField, (Collection) argsIterator.next()));
            break;
        }

        return condition;
    }


    @Override
    public Projection buildProjection(String projectionField,
                                      ProjectionType projectionType,
                                      Iterator argsIterator) {
        Projection projection = new Projection();

        projection.setField(projectionField);

        switch (projectionType) {
        case RowCount:
            projection.setDetails(Projections.rowCount());
            break;

        case Count:
            projection.setDetails(Projections.count(projectionField));
            break;

        case CountDistinct:
            projection.setDetails(Projections.countDistinct(projectionField));
            break;

        case Maximum:
            projection.setDetails(Projections.max(projectionField));
            break;

        case Minimum:
            projection.setDetails(Projections.min(projectionField));
            break;

        case Average:
            projection.setDetails(Projections.avg(projectionField));
            break;

        case Sum:
            projection.setDetails(Projections.sum(projectionField));
            break;

        default:
            throw new RuntimeException("Unexpected projection type: " + projectionType);
        }

        return projection;
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected Session getCurrentSession() {
        return sessionFactory.getCurrentSession();
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private Criteria buildCriteria(QueryModel queryModel) {
        Criteria criteria = getCurrentSession().createCriteria(persistentClass);

        if (queryModel.getConditions() != null) {
            for (Condition condition : queryModel.getConditions()) {
                criteria.add((Criterion) condition.getConstraint());
            }
        }

        for (Map.Entry<String, List<Condition>> associationCriteriaEntry : queryModel.getAssociationConditions().entrySet()) {
            Criteria associationCriteria = criteria.createCriteria(associationCriteriaEntry.getKey());

            criteria.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);

            for (Condition condition : associationCriteriaEntry.getValue()) {
                associationCriteria.add((Criterion) condition.getConstraint());
            }
        }

        if (queryModel.getProjection() != null) {
            ProjectionList projectionList = Projections.projectionList();

            projectionList.add((org.hibernate.criterion.Projection) queryModel.getProjection().getDetails());

            criteria.setProjection(projectionList);
        }

        return criteria;
    }
}
