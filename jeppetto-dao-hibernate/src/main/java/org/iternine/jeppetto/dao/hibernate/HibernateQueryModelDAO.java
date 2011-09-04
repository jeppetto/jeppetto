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

package org.iternine.jeppetto.dao.hibernate;


import org.iternine.jeppetto.dao.AccessControlContext;
import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.AccessControllable;
import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.ConditionType;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.Projection;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.QueryModel;
import org.iternine.jeppetto.dao.QueryModelDAO;
import org.iternine.jeppetto.dao.Sort;
import org.iternine.jeppetto.dao.SortDirection;
import org.iternine.jeppetto.dao.annotation.AccessControl;
import org.iternine.jeppetto.dao.annotation.AccessControlRule;
import org.iternine.jeppetto.dao.annotation.AccessControlType;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.engine.SessionFactoryImplementor;
import org.hibernate.engine.TypedValue;
import org.hibernate.impl.CriteriaImpl;
import org.hibernate.loader.criteria.CriteriaQueryTranslator;
import org.hibernate.type.StringType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * An implementation of the QueryModelDAO interface that supports Hibernate.
 *
 * @param <T> persistent class.
 * @param <ID> ID type for the persistent class.
 */
public class HibernateQueryModelDAO<T, ID extends Serializable>
        implements QueryModelDAO<T, ID>, AccessControllable<ID> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final StringType STRING_TYPE = new StringType();


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Class<T> persistentClass;
    private SessionFactory sessionFactory;
    private AccessControlEntryHelper accessControlEntryHelper;
    private AccessControlContextProvider accessControlContextProvider;
    private String idField = "id";      // TODO: Allow for configuration...


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public HibernateQueryModelDAO(Class<T> persistentClass, Map<String, Object> daoProperties) {
        this(persistentClass, daoProperties, null);
    }


    public HibernateQueryModelDAO(Class<T> persistentClass, Map<String, Object> daoProperties,
                                  AccessControlContextProvider accessControlContextProvider) {
        this.persistentClass = persistentClass;
        this.sessionFactory = (SessionFactory) daoProperties.get("sessionFactory");
        this.accessControlEntryHelper = (AccessControlEntryHelper) daoProperties.get("accessControlEntryHelper");
        this.accessControlContextProvider = accessControlContextProvider;
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(ID id)
            throws NoSuchItemException {
        QueryModel queryModel = new QueryModel();
        queryModel.addCondition(buildIdCondition(id));
        
        if (accessControlContextProvider != null) {
            queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
        }

        return findUniqueUsingQueryModel(queryModel);
    }


    @Override
    public Iterable<T> findAll() {
        QueryModel queryModel = new QueryModel();

        if (accessControlContextProvider != null) {
            queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
        }

        return findUsingQueryModel(queryModel);
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
        T result;

        if (accessControlContextProvider == null || roleAllowsAccess(queryModel.getAccessControlContext().getRole())) {
            // noinspection unchecked
            result = (T) buildCriteria(queryModel).uniqueResult();
        } else {
            // noinspection unchecked
            result = (T) createEntryBasedQuery(queryModel).uniqueResult();
        }

        if (result == null) {
            throw new NoSuchItemException(persistentClass.getSimpleName(), queryModel.toString());
        }

        return result;
    }


    @Override
    public Iterable<T> findUsingQueryModel(QueryModel queryModel) {
        if (accessControlContextProvider == null || roleAllowsAccess(queryModel.getAccessControlContext().getRole())) {
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
        } else {
            //noinspection unchecked
            return createEntryBasedQuery(queryModel).list();
        }
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
    // Implementation - AccessControllable
    //-------------------------------------------------------------

    @Override
    public AccessControlContextProvider getAccessControlContextProvider() {
        return accessControlContextProvider;
    }


    @Override
    public void grantAccess(ID id, String accessId) {
        accessControlEntryHelper.createEntry(persistentClass, id, accessId);
    }


    @Override
    public void revokeAccess(ID id, String accessId) {
        accessControlEntryHelper.deleteEntry(persistentClass, id, accessId);
    }


    @Override
    public List<String> getAccessIds(ID id) {
        return accessControlEntryHelper.getEntries(persistentClass, id);
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected Session getCurrentSession() {
        return sessionFactory.getCurrentSession();
    }


    protected Condition buildIdCondition(ID id) {
        Condition condition = new Condition();

        condition.setField(idField);
        condition.setConstraint(Restrictions.eq(idField, id));

        return condition;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private boolean roleAllowsAccess(String role) {
        AccessControl accessControl;

        if (role == null || role.isEmpty()) {
            return false;
        }

        if ((accessControl = getAccessControlAnnotation()) == null) {
            return false;
        }

        for (AccessControlRule accessControlRule : accessControl.rules()) {
            if (accessControlRule.type() == AccessControlType.Role && accessControlRule.value().equals(role)) {
                return true;
            }
        }

        return false;
    }


    private AccessControl getAccessControlAnnotation() {
        Class classToExamine = persistentClass;

        while (classToExamine != null) {
            // noinspection unchecked
            AccessControl accessControl = (AccessControl) classToExamine.getAnnotation(AccessControl.class);

            if (accessControl != null) {
                return accessControl;
            }

            classToExamine = classToExamine.getSuperclass();
        }

        return null;
    }


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



    // TODO: Add projection, maxResults, firstResult support
    private Query createEntryBasedQuery(QueryModel queryModel) {
        Criteria criteria = getCurrentSession().createCriteria(persistentClass);

        for (String associationPath : queryModel.getAssociationConditions().keySet()) {
            criteria.createCriteria(associationPath);
        }

        CriteriaQueryTranslator criteriaQueryTranslator = new CriteriaQueryTranslator((SessionFactoryImplementor) sessionFactory,
                                                                                      (CriteriaImpl) criteria,
                                                                                      persistentClass.getName(),
                                                                                      CriteriaQueryTranslator.ROOT_SQL_ALIAS);

        StringBuilder queryStringBuilder = new StringBuilder();

        buildSelectClause(queryStringBuilder, criteriaQueryTranslator, queryModel.getAssociationConditions().keySet());

        List<TypedValue> parameters = buildWhereClause(queryStringBuilder, queryModel, criteria, criteriaQueryTranslator);

        if ((queryModel.getAssociationConditions() == null || queryModel.getAssociationConditions().isEmpty())
            && (queryModel.getSorts() == null || queryModel.getSorts().isEmpty())) {
            buildDefaultOrderClause(queryStringBuilder);
        } else {
            // can't use the default ordering by "ace.id" because of "select distinct..." syntax
            buildOrderClause(queryStringBuilder, queryModel, criteria, criteriaQueryTranslator);
        }

        Query query = getCurrentSession().createQuery(queryStringBuilder.toString());

        setParameters(parameters, query, queryModel.getAccessControlContext());

        return query;
    }


    private void buildSelectClause(StringBuilder queryStringBuilder, CriteriaQueryTranslator criteriaQueryTranslator,
                                   Set<String> associationPaths) {
        // "distinct" is needed only for association criteria, but it prevents specifying ordering by "ace.id"
        if (associationPaths.isEmpty()) {
            queryStringBuilder.append("select ");
        } else {
            queryStringBuilder.append("select distinct ");
        }
        queryStringBuilder.append(criteriaQueryTranslator.getRootSQLALias());
        queryStringBuilder.append(" from AccessControlEntry ace, ");
        queryStringBuilder.append(persistentClass.getSimpleName());
        queryStringBuilder.append(' ');
        queryStringBuilder.append(criteriaQueryTranslator.getRootSQLALias());

        for (String associationPath : associationPaths) {
            queryStringBuilder.append(" join ");
            queryStringBuilder.append(criteriaQueryTranslator.getRootSQLALias());
            queryStringBuilder.append('.');
            queryStringBuilder.append(associationPath);
            queryStringBuilder.append(" as ");
            queryStringBuilder.append(criteriaQueryTranslator.getSQLAlias(criteriaQueryTranslator.getCriteria(associationPath)));
        }
    }


    private List<TypedValue> buildWhereClause(StringBuilder queryStringBuilder, QueryModel queryModel,
                                              Criteria criteria, CriteriaQueryTranslator criteriaQueryTranslator) {
        List<TypedValue> parameters = new ArrayList<TypedValue>();

        queryStringBuilder.append(" where ");

        if (queryModel.getConditions() != null) {
            for (Condition condition : queryModel.getConditions()) {
                Criterion criterion = (Criterion) condition.getConstraint();

                queryStringBuilder.append(criterion.toSqlString(criteria, criteriaQueryTranslator));
                queryStringBuilder.append(" and ");

                parameters.addAll(Arrays.asList(criterion.getTypedValues(criteria, criteriaQueryTranslator)));
            }
        }

        if (queryModel.getAssociationConditions() != null) {
            for (Map.Entry<String, List<Condition>> associationCriteriaEntry : queryModel.getAssociationConditions().entrySet()) {
                CriteriaImpl.Subcriteria associationCriteria
                        = (CriteriaImpl.Subcriteria) criteriaQueryTranslator.getCriteria(associationCriteriaEntry.getKey());

                for (Condition condition : associationCriteriaEntry.getValue()) {
                    Criterion criterion = (Criterion) condition.getConstraint();

                    queryStringBuilder.append(criterion.toSqlString(associationCriteria, criteriaQueryTranslator));
                    queryStringBuilder.append(" and ");

                    parameters.addAll(Arrays.asList(criterion.getTypedValues(associationCriteria, criteriaQueryTranslator)));
                }
            }
        }

        queryStringBuilder.append(" ace.objectType = '");
        queryStringBuilder.append(persistentClass.getSimpleName());
        queryStringBuilder.append("' and ace.objectId = ");
        queryStringBuilder.append(criteriaQueryTranslator.getRootSQLALias());
        queryStringBuilder.append('.');
        queryStringBuilder.append(idField);
        queryStringBuilder.append(" and ace.accessibleBy = ? ");

        return parameters;
    }


    private void buildOrderClause(StringBuilder queryStringBuilder, QueryModel queryModel, Criteria criteria,
                                  CriteriaQueryTranslator criteriaQueryTranslator) {
        boolean firstOrderItem = true;

        if (queryModel.getSorts() != null) {
            for (Sort sort : queryModel.getSorts()) {
                if (firstOrderItem) {
                    queryStringBuilder.append(" order by ");
                } else {
                    queryStringBuilder.append(',');
                }

                Order order = sort.getSortDirection() == SortDirection.Ascending ? Order.asc(sort.getField())
                                                                                 : Order.desc(sort.getField());

                queryStringBuilder.append(order.toSqlString(criteria, criteriaQueryTranslator));

                firstOrderItem = false;
            }
        }
    }


    /**
     * Default ordering by ACE.id field (integer) to ensure older-to-newer entries
     *
     * @param queryStringBuilder main query string tbeing built
     */
    private void buildDefaultOrderClause(StringBuilder queryStringBuilder) {
        queryStringBuilder.append(" order by ace.id asc");
    }


    private void setParameters(List<TypedValue> parameters, Query query, AccessControlContext accessControlContext) {
        int position = 0;

        for (TypedValue parameter : parameters) {
            query.setParameter(position++, parameter.getValue(), parameter.getType());
        }

        query.setParameter(position, accessControlContext.getAccessId(), STRING_TYPE);
    }
}
