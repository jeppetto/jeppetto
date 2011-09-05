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

package org.iternine.jeppetto.dao.jdbc;


import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.ConditionType;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.Projection;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.QueryModel;
import org.iternine.jeppetto.dao.QueryModelDAO;
import org.iternine.jeppetto.dao.Sort;
import org.iternine.jeppetto.dao.SortDirection;
import org.iternine.jeppetto.dao.id.IdGenerator;
import org.iternine.jeppetto.dao.jdbc.enhance.EnhancerHelper;
import org.iternine.jeppetto.dao.jdbc.enhance.JDBCPersistable;
import org.iternine.jeppetto.enhance.Enhancer;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
// TODO: dirtyable support
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
            throws NoSuchItemException {
        QueryModel queryModel = new QueryModel();

        queryModel.addCondition(buildCondition("id", ConditionType.Equal, Collections.singletonList(id).iterator()));

        return findUniqueUsingQueryModel(queryModel);
    }


    @Override
    public Iterable<T> findAll() {
        return findUsingQueryModel(new QueryModel());
    }


    @Override
    public void save(T entity) {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            ((JDBCPersistable) enhancer.enhance(entity)).save(connection, idGenerator);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) { try { connection.close(); } catch (SQLException ignore) { } }
        }
    }


    @Override
    public void delete(T entity) {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            // TODO: support cascading deletes
            ((JDBCPersistable) enhancer.enhance(entity)).delete(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) { try { connection.close(); } catch (SQLException ignore) { } }
        }
    }


    @Override
    public void deleteById(ID id) {
        // Create a lightweight entity instance to use for delete()
        T entity = enhancer.newInstance();

        try {
            entity.getClass().getMethod("setId").invoke(entity, id);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        delete(entity);
    }


    @Override
    public void flush() {
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    @Override
    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException {
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
                throw new RuntimeException("More than one " + entityClass.getSimpleName()
                                           + " matches query: " + buildSelectString(queryModel));
            }

            return t;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (resultSet != null) { try { resultSet.close(); } catch (SQLException ignore) { } }
            if (preparedStatement != null) { try { preparedStatement.close(); } catch (SQLException ignore) { } }
            if (connection != null) { try { connection.close(); } catch (SQLException ignore) { } }
        }
    }


    @Override
    public Iterable<T> findUsingQueryModel(QueryModel queryModel) {
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
            throw new RuntimeException(e);
        } finally {
            if (resultSet != null) { try { resultSet.close(); } catch (SQLException ignore) { } }
            if (preparedStatement != null) { try { preparedStatement.close(); } catch (SQLException ignore) { } }
            if (connection != null) { try { connection.close(); } catch (SQLException ignore) { } }
        }
    }


    @Override
    public Object projectUsingQueryModel(QueryModel queryModel) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public Condition buildCondition(String conditionField, ConditionType conditionType, Iterator argsIterator) {
        JDBCCondition jdbcCondition = JDBCCondition.valueOf(conditionType.name());

        return new Condition(conditionField, jdbcCondition.buildConstraint(argsIterator));
    }


    @Override
    public Projection buildProjection(String projectionField, ProjectionType projectionType, Iterator argsIterator) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
