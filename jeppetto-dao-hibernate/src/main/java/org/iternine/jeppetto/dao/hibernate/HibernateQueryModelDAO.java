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

package org.iternine.jeppetto.dao.hibernate;


import org.iternine.jeppetto.dao.AccessControlContext;
import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.AccessControlDAO;
import org.iternine.jeppetto.dao.AccessControlException;
import org.iternine.jeppetto.dao.AccessType;
import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.ConditionType;
import org.iternine.jeppetto.dao.FailedBatchException;
import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.OptimisticLockException;
import org.iternine.jeppetto.dao.Projection;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.QueryModel;
import org.iternine.jeppetto.dao.QueryModelDAO;
import org.iternine.jeppetto.dao.Sort;
import org.iternine.jeppetto.dao.SortDirection;
import org.iternine.jeppetto.dao.TooManyItemsException;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StaleObjectStateException;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.TypedValue;
import org.hibernate.internal.CriteriaImpl;
import org.hibernate.loader.criteria.CriteriaQueryTranslator;
import org.hibernate.type.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
        implements QueryModelDAO<T, ID>, AccessControlDAO<T, ID> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final StringType STRING_TYPE = new StringType();


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Class<T> persistentClass;
    private SessionFactory sessionFactory;
    private AccessControlHelper accessControlHelper;
    private AccessControlContextProvider accessControlContextProvider;
    private String idField = "id";      // TODO: Allow for configuration...

    private static final Logger logger = LoggerFactory.getLogger(HibernateQueryModelDAO.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected HibernateQueryModelDAO(Class<T> persistentClass, Map<String, Object> daoProperties) {
        this(persistentClass, daoProperties, null);
    }


    protected HibernateQueryModelDAO(Class<T> persistentClass, Map<String, Object> daoProperties,
                                     AccessControlContextProvider accessControlContextProvider) {
        this.persistentClass = persistentClass;
        this.sessionFactory = (SessionFactory) daoProperties.get("sessionFactory");
        this.accessControlHelper = (AccessControlHelper) daoProperties.get("accessControlHelper");
        this.accessControlContextProvider = accessControlContextProvider;

        if (accessControlHelper != null) {
            accessControlHelper.registerDAO(persistentClass, this);
        }
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(ID id)
            throws NoSuchItemException, JeppettoException {
        QueryModel queryModel = new QueryModel();
        queryModel.addCondition(buildIdCondition(id));
        
        if (accessControlContextProvider != null) {
            queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
        }

        return findUniqueUsingQueryModel(queryModel);
    }


    @Override
    public Iterable<T> findByIds(ID... ids)
            throws JeppettoException {
        QueryModel queryModel = new QueryModel();
        queryModel.addCondition(buildIdCondition(Arrays.asList(ids)));

        if (accessControlContextProvider != null) {
            queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
        }

        return findUsingQueryModel(queryModel);
    }


    @Override
    public Iterable<T> findAll()
            throws JeppettoException {
        QueryModel queryModel = new QueryModel();

        if (accessControlContextProvider != null) {
            queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
        }

        return findUsingQueryModel(queryModel);
    }


    @Override
    public void save(T entity)
            throws OptimisticLockException, JeppettoException {
        try {
            getCurrentSession().saveOrUpdate(entity);

            flush();
        } catch (org.hibernate.OptimisticLockException e) {
            throw new OptimisticLockException(e);
        } catch (StaleObjectStateException e) {
            throw new OptimisticLockException(e);
        } catch (HibernateException e) {
            throw new JeppettoException(e);
        }
    }


    @Override
    public void delete(T entity)
            throws JeppettoException {
        try {
            getCurrentSession().delete(entity);
        } catch (HibernateException e) {
            throw new JeppettoException(e);
        }
    }


    @Override
    public void deleteById(ID id)
            throws JeppettoException {
        try {
            getCurrentSession().delete(findById(id));
        } catch (NoSuchItemException ignore) {
            // If it doesn't exist, no need to delete.
        } catch (HibernateException e) {
            throw new JeppettoException(e);
        }
    }


    @Override
    public void deleteByIds(ID... ids)
            throws FailedBatchException, JeppettoException {
        Map<ID, Exception> failedDeletes = new LinkedHashMap<ID, Exception>();

        for (ID id : ids) {
            try {
                deleteById(id);
            } catch (Exception e) {
                failedDeletes.put(id, e);
            }
        }

        if (failedDeletes.size() > 0) {
            // TODO: fix emptyList()...
            throw new FailedBatchException("Unable to delete all items", Collections.emptyList(), failedDeletes);
        }
    }


    @Override
    public <U extends T> U getUpdateObject() {
        throw new RuntimeException("getUpdateObject not yet implemented");
    }


    @Override
    public <U extends T> T updateById(U updateObject, ID id)
            throws JeppettoException {
        throw new RuntimeException("updateById not yet implemented");
    }


    @Override
    public <U extends T> Iterable<T> updateByIds(U updateObject, ID... ids)
            throws FailedBatchException, JeppettoException {
        throw new RuntimeException("updateByIds not yet implemented");
    }


    @Override
    public void flush()
            throws JeppettoException {
        getCurrentSession().flush();
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    @Override
    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException, TooManyItemsException, JeppettoException {
        T result;

        try {
            if (accessControlContextProvider == null
                || accessControlHelper.annotationAllowsAccess(persistentClass, queryModel.getAccessControlContext(), AccessType.Read)) {
                // noinspection unchecked
                result = (T) buildCriteria(queryModel).uniqueResult();
            } else {
                // noinspection unchecked
                result = (T) createAccessControlledQuery(queryModel).uniqueResult();
            }
        } catch (NonUniqueObjectException e) {
            throw new TooManyItemsException(e.getMessage());
        } catch (HibernateException e) {
            throw new JeppettoException(e);
        }

        if (result == null) {
            throw new NoSuchItemException(persistentClass.getSimpleName(), queryModel.toString());
        }

        return result;
    }


    @Override
    public Iterable<T> findUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        try {
            if (accessControlContextProvider == null
                || accessControlHelper.annotationAllowsAccess(persistentClass, queryModel.getAccessControlContext(), AccessType.Read)) {
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
                return createAccessControlledQuery(queryModel).list();
            }
        } catch (HibernateException e) {
            throw new JeppettoException(e);
        }
    }


    @Override
    public Object projectUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        try {
            return buildCriteria(queryModel).uniqueResult();
        } catch (HibernateException e) {
            throw new JeppettoException(e);
        }
    }


    @Override
    public void deleteUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        throw new RuntimeException("deleteUsingQueryModel not yet implemented");
    }


    @Override
    public <U extends T> T updateUniqueUsingQueryModel(U updateObject, QueryModel queryModel)
            throws JeppettoException {
        throw new RuntimeException("updateUniqueUsingQueryModel not yet implemented");
    }


    @Override
    public <U extends T> Iterable<T> updateUsingQueryModel(U updateObject, QueryModel queryModel)
            throws JeppettoException {
        throw new RuntimeException("updateUsingQueryModel not yet implemented");
    }


    @Override
    public Condition buildCondition(String conditionField, ConditionType conditionType, Iterator argsIterator) {
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

        case BeginsWith:
            // TODO: Escape argsIterator.next()
            condition.setConstraint(Restrictions.like(conditionField, argsIterator.next().toString() + '%'));
            break;
        }

        return condition;
    }


    @Override
    public Projection buildProjection(String projectionField, ProjectionType projectionType, Iterator argsIterator) {
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
    // Implementation - AccessControlDAO
    //-------------------------------------------------------------

    @Override
    public void save(T object, AccessControlContext accessControlContext)
            throws OptimisticLockException, AccessControlException, JeppettoException {
        ensureAccessControlEnabled();

        try {
            AccessControlContextOverride.set(accessControlContext);

            getCurrentSession().saveOrUpdate(object);

            // flush() here because we want the AccessControlInterceptor to perform its onSave()/onFlushDirty()
            // checks while the override is in place.
            flush();
        } catch (org.hibernate.OptimisticLockException e) {
            throw new OptimisticLockException(e);
        } catch (HibernateException e) {
            throw new JeppettoException(e);
        } finally {
            AccessControlContextOverride.clear();
        }
    }


    @Override
    public void grantAccess(ID id, String accessId, AccessType accessType)
            throws NoSuchItemException, AccessControlException {
        ensureAccessControlEnabled();

        grantAccess(id, accessId, accessType, accessControlContextProvider.getCurrent());
    }


    @Override
    public void grantAccess(ID id, String accessId, AccessType accessType, AccessControlContext accessControlContext)
            throws NoSuchItemException, AccessControlException {
        ensureAccessControlEnabled();

        if (accessType == AccessType.None) {
            revokeAccess(id, accessId, accessControlContext);

            return;
        }

        accessControlHelper.validateContextAllowsWrite(persistentClass, id, accessControlContext, true);

        accessControlHelper.createEntry(persistentClass, id, accessId, accessType);
    }


    @Override
    public void revokeAccess(ID id, String accessId)
            throws NoSuchItemException, AccessControlException {
        ensureAccessControlEnabled();

        revokeAccess(id, accessId, accessControlContextProvider.getCurrent());
    }


    @Override
    public void revokeAccess(ID id, String accessId, AccessControlContext accessControlContext)
            throws NoSuchItemException, AccessControlException {
        ensureAccessControlEnabled();

        accessControlHelper.validateContextAllowsWrite(persistentClass, id, accessControlContext, true);

        accessControlHelper.deleteEntry(persistentClass, id, accessId);
    }


    @Override
    public Map<String, AccessType> getGrantedAccesses(ID id)
            throws NoSuchItemException, AccessControlException {
        ensureAccessControlEnabled();

        return getGrantedAccesses(id, accessControlContextProvider.getCurrent());
    }


    @Override
    public Map<String, AccessType> getGrantedAccesses(ID id, AccessControlContext accessControlContext)
            throws NoSuchItemException, AccessControlException {
        ensureAccessControlEnabled();

        accessControlHelper.validateContextAllowsWrite(persistentClass, id, accessControlContext, true);

        return accessControlHelper.getEntries(persistentClass, id);
    }


    @Override
    public AccessControlContextProvider getAccessControlContextProvider() {
        return accessControlContextProvider;
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected Session getCurrentSession() {
        return sessionFactory.getCurrentSession();
    }


    protected Condition buildIdCondition(Object argument) {
        if (Collection.class.isAssignableFrom(argument.getClass())) {
            return new Condition(idField, Restrictions.in(idField, (Collection) argument));
        } else {
            return new Condition(idField, Restrictions.eq(idField, argument));
        }
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



    // TODO: Add projection, maxResults, firstResult support
    private Query createAccessControlledQuery(QueryModel queryModel) {
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


    private void ensureAccessControlEnabled() {
        if (accessControlContextProvider == null) {
            throw new AccessControlException("Access Control is not enabled. No AccessControlContextProvider specified.");
        }
    }
}
