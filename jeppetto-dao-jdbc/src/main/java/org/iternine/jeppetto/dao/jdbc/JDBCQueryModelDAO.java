/*
 * Copyright (c) 2011-2014 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.jdbc;


import org.iternine.jeppetto.dao.AccessControlContextProvider;
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
import org.iternine.jeppetto.dao.id.IdGenerator;
import org.iternine.jeppetto.dao.jdbc.enhance.EnhancerHelper;
import org.iternine.jeppetto.dao.jdbc.enhance.JDBCPersistable;
import org.iternine.jeppetto.enhance.Enhancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of the QueryModelDAO interface that works atop JDBC.
 *
 * @param <T> Persistent class
 * @param <ID> ID type of the persistent class.
 */
// TODO: updates
// TODO: sessions?
// TODO: collections
// TODO: associated classes
// TODO: projections
// TODO: ACLs
// TODO: delete
// TODO: transaction support
// TODO: better 'iterable' support
// TODO: other id generation schemes
// TODO: persistable support
// TODO: move EnhancerHelper to jeppetto-enhance
// TODO: olv support
public class JDBCQueryModelDAO<T, ID>
        implements QueryModelDAO<T, ID> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Class<T> entityClass;
    private Enhancer<T> enhancer;
    private DataSource dataSource;
    private IdGenerator<ID> idGenerator;
    private AccessControlContextProvider accessControlContextProvider;

    private static final Logger logger = LoggerFactory.getLogger(JDBCQueryModelDAO.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected JDBCQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties) {
        this(entityClass, daoProperties, null);
    }


    protected JDBCQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties,
                                AccessControlContextProvider accessControlContextProvider) {
        this.entityClass = entityClass;
        this.enhancer = EnhancerHelper.getJDBCPersistableEnhancer(entityClass);
        this.dataSource = (DataSource) daoProperties.get("dataSource");
        this.idGenerator = (IdGenerator<ID>) daoProperties.get("idGenerator");
        this.accessControlContextProvider = accessControlContextProvider;
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(ID id)
            throws NoSuchItemException, JeppettoException {
        QueryModel queryModel = new QueryModel();

        queryModel.addCondition(buildCondition("id", ConditionType.Equal, Collections.singletonList(id).iterator()));

        return findUniqueUsingQueryModel(queryModel);
    }


    @Override
    public Iterable<T> findByIds(ID... ids)
            throws JeppettoException {
        QueryModel queryModel = new QueryModel();

        queryModel.addCondition(buildCondition("id", ConditionType.Within, Collections.singletonList(Arrays.asList(ids)).iterator()));

        return findUsingQueryModel(queryModel);
    }


    @Override
    public Iterable<T> findAll()
            throws JeppettoException {
        return findUsingQueryModel(new QueryModel());
    }


    @Override
    public void save(T entity)
            throws OptimisticLockException, JeppettoException {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            ((JDBCPersistable) enhancer.enhance(entity)).save(connection, idGenerator);
        } catch (SQLException e) {
            throw new JeppettoException(e);
        } finally {
            if (connection != null) { try { connection.close(); } catch (SQLException ignore) { } }
        }
    }


    @Override
    public void delete(T entity)
            throws JeppettoException {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            // TODO: support cascading deletes
            ((JDBCPersistable) enhancer.enhance(entity)).delete(connection);
        } catch (SQLException e) {
            throw new JeppettoException(e);
        } finally {
            if (connection != null) { try { connection.close(); } catch (SQLException ignore) { } }
        }
    }


    @Override
    public void deleteById(ID id)
            throws JeppettoException {
        // Create a lightweight entity instance to use for delete()
        T entity = enhancer.newInstance();

        try {
            entity.getClass().getMethod("setId").invoke(entity, id);
        } catch (Exception e) {
            throw new JeppettoException(e);
        }

        delete(entity);
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
            throw new FailedBatchException("Unable to delete all items", failedDeletes);
        }
    }


    @Override
    public <U extends T> U getUpdateObject() {
        throw new RuntimeException("getUpdateObject not yet implemented");
    }


    @Override
    public <U extends T> void updateByIds(U updateObject, ID... ids)
            throws FailedBatchException, JeppettoException {
        throw new RuntimeException("updateByIds not yet implemented");
    }


    @Override
    public void flush()
            throws JeppettoException {
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    @Override
    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException, TooManyItemsException, JeppettoException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = dataSource.getConnection();
            preparedStatement = buildPreparedStatement(connection, queryModel);
            resultSet = preparedStatement.executeQuery();

            if (!resultSet.next()) {
                throw new NoSuchItemException(entityClass.getSimpleName(), buildSelectString(queryModel));
            }

            T t = enhancer.newInstance();

            ((JDBCPersistable) t).populateObject(resultSet);

            if (resultSet.next()) {
                throw new TooManyItemsException(entityClass.getSimpleName(), buildSelectString(queryModel));
            }

            return t;
        } catch (SQLException e) {
            throw new JeppettoException(e);
        } finally {
            if (resultSet != null) { try { resultSet.close(); } catch (SQLException ignore) { } }
            if (preparedStatement != null) { try { preparedStatement.close(); } catch (SQLException ignore) { } }
            if (connection != null) { try { connection.close(); } catch (SQLException ignore) { } }
        }
    }


    @Override
    public Iterable<T> findUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = dataSource.getConnection();
            preparedStatement = buildPreparedStatement(connection, queryModel);
            resultSet = preparedStatement.executeQuery();

            List<T> result = new ArrayList<T>();

            while (resultSet.next()) {
                T t = enhancer.newInstance();

                ((JDBCPersistable) t).populateObject(resultSet);

                result.add(t);
            }

            return result;
        } catch (SQLException e) {
            throw new JeppettoException(e);
        } finally {
            if (resultSet != null) { try { resultSet.close(); } catch (SQLException ignore) { } }
            if (preparedStatement != null) { try { preparedStatement.close(); } catch (SQLException ignore) { } }
            if (connection != null) { try { connection.close(); } catch (SQLException ignore) { } }
        }
    }


    @Override
    public Object projectUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        throw new RuntimeException("projectUsingQueryModel not yet implemented");
    }


    @Override
    public void deleteUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        throw new RuntimeException("deleteUsingQueryModel not yet implemented");
    }


    @Override
    public <U extends T> void updateUsingQueryModel(U updateObject, QueryModel queryModel)
            throws JeppettoException {
        throw new RuntimeException("updateUsingQueryModel not yet implemented");
    }


    @Override
    public Condition buildCondition(String conditionField, ConditionType conditionType, Iterator argsIterator) {
        JDBCCondition jdbcCondition = JDBCCondition.valueOf(conditionType.name());

        return new Condition(conditionField, jdbcCondition.buildConstraint(argsIterator));
    }


    @Override
    public Projection buildProjection(String projectionField, ProjectionType projectionType, Iterator argsIterator) {
        return null;  // Todo: implement
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private PreparedStatement buildPreparedStatement(Connection connection, QueryModel queryModel)
            throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(buildSelectString(queryModel));

        int parameterLocation = 1;
        for (Condition condition : queryModel.getConditions()) {
            JDBCConstraint jdbcConstraint = (JDBCConstraint) condition.getConstraint();

            if (jdbcConstraint.getParameter1() != null) {
                preparedStatement.setObject(parameterLocation++, jdbcConstraint.getParameter1());
            }

            if (jdbcConstraint.getParameter2() != null) {
                preparedStatement.setObject(parameterLocation++, jdbcConstraint.getParameter2());
            }
        }

        return preparedStatement;
    }


    private String buildSelectString(QueryModel queryModel) {
        StringBuilder selectClause = new StringBuilder("SELECT * FROM " + entityClass.getSimpleName());

        if (queryModel.getConditions() != null && !queryModel.getConditions().isEmpty()) {
            selectClause.append(" WHERE ");

            boolean first = true;

            for (Condition condition : queryModel.getConditions()) {
                if (!first) {
                    selectClause.append(" AND ");
                }

                selectClause.append(condition.getField());
                selectClause.append(((JDBCConstraint) condition.getConstraint()).getConstraintString());

                first = false;
            }
        }

        if (queryModel.getSorts() != null && !queryModel.getSorts().isEmpty()) {
            selectClause.append(" ORDER BY ");

            boolean first = true;

            for (Sort sort : queryModel.getSorts()) {
                if (!first) {
                    selectClause.append(" AND ");
                }

                selectClause.append(sort.getField());

                if (sort.getSortDirection() == SortDirection.Descending) {
                    selectClause.append(" DESC ");
                }

                first = false;
            }
        }

        if (queryModel.getMaxResults() > 0) {
            selectClause.append(" LIMIT ");
            selectClause.append(queryModel.getMaxResults());
        }

        if (queryModel.getFirstResult() > 0) {
            selectClause.append(" OFFSET ");
            selectClause.append(queryModel.getFirstResult());
        }

        return selectClause.toString();
    }
}
