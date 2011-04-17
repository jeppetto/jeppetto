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


import org.jeppetto.dao.Condition;
import org.jeppetto.dao.ConditionType;
import org.jeppetto.dao.NoSuchItemException;
import org.jeppetto.dao.Projection;
import org.jeppetto.dao.ProjectionType;
import org.jeppetto.dao.QueryModel;
import org.jeppetto.dao.QueryModelDAO;
import org.jeppetto.dao.Sort;
import org.jeppetto.dao.SortDirection;
import org.jeppetto.dao.mongodb.enhance.DBObjectUtil;
import org.jeppetto.dao.mongodb.enhance.Dirtyable;
import org.jeppetto.dao.mongodb.enhance.EnhancerHelper;
import org.jeppetto.dao.mongodb.enhance.MongoDBCallback;
import org.jeppetto.dao.mongodb.projections.ProjectionCommands;
import org.jeppetto.enhance.Enhancer;
import org.jeppetto.enhance.ExceptionUtil;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCallback;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;


/**
 * Provides a QueryModelDAO implementation for Mongo.
 *
 * Provides bi-directional ODM (Object to Document Mapping) as well as the
 * rich query capabilities found in the core org.jeppetto.dao package.
 *
 * This implementation of the QueryModelDAO requires that the PK be a String.
 *
 * Instantiation requires a daoProperties Map<String, Object> that will look
 * for the following keys (and expected values):
 *
 *   "db" -> Instance of a configured com.mongodb.DB        (required)
 *   "uniqueIndexes" -> List<String> of various MongoDB     (optional)
 *                      index values that will be
 *                      ensured to exist and must be
 *                      unique
 *   "nonUniqueIndexes" -> List<String> of various MongoDB  (optional)
 *                         index values that will be
 *                         ensured to exist and need
 *                         not be unique
 *   "optimisticLockEnabled" -> Boolean as to whether       (optional)
 *                              instances of the tracked
 *                              type should have an
 *                              implicit lock version
 *                              field
 *   "showQueries" -> Boolean that indicates if additional  (optional)
 *                    info should be logged regarding the
 *                    various queries that are executed.
 *                    Logging will need need to be enabled
 *                    for the DAO's package
 *
 * @param <T> the type of persistent object this DAO will manage.
 */
// TODO: support createdDate/lastModifiedDate (createdDate from get("_id").getTime()?)
public class MongoDBQueryModelDAO<T>
        implements QueryModelDAO<T, String> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final String ID_FIELD = "_id";
    private static final String OPTIMISTIC_LOCK_VERSION_FIELD = "__olv";
    private static final Enhancer<MongoDBError> ERROR_ENHANCER = EnhancerHelper.getDBObjectEnhancer(MongoDBError.class);
    private static final Map<ConditionType, MongoDBOperator> CONDITION_TYPE_TO_MONGO_OPERATOR = new HashMap<ConditionType, MongoDBOperator>() { {
            put(ConditionType.Equal, MongoDBOperator.Equal);
            put(ConditionType.NotEqual, MongoDBOperator.NotEqual);
            put(ConditionType.GreaterThan, MongoDBOperator.GreaterThan);
            put(ConditionType.GreaterThanEqual, MongoDBOperator.GreaterThanEqual);
            put(ConditionType.LessThan, MongoDBOperator.LessThan);
            put(ConditionType.LessThanEqual, MongoDBOperator.LessThanEqual);
            put(ConditionType.Between, MongoDBOperator.Between);
            put(ConditionType.Within, MongoDBOperator.Within);
            put(ConditionType.NotWithin, MongoDBOperator.NotWithin);
//            put(ConditionType.IsNull, MongoDBOperator.IsNull);
//            put(ConditionType.IsNotNull, MongoDBOperator.IsNotNull);
        }
    };


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private DB db;
    private DBCollection dbCollection;
    private Enhancer<T> enhancer;
    private Map<String, Set<String>> uniqueIndexes;
    private boolean optimisticLockEnabled;
    private boolean showQueries;
    private Logger queryLogger = LoggerFactory.getLogger(getClass());


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    @SuppressWarnings( { "unchecked" })
    protected MongoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties) {
        this.db = (DB) daoProperties.get("db");
        this.dbCollection = db.getCollection(entityClass.getSimpleName());
        this.enhancer = EnhancerHelper.getDirtyableDBObjectEnhancer(entityClass);

        dbCollection.setObjectClass(enhancer.getEnhancedClass());

        DBCallback.FACTORY = new DBCallback.Factory() {
            @Override
            public DBCallback create(DBCollection dbCollection) {
                return new MongoDBCallback(dbCollection);
            }
        };

        this.uniqueIndexes = ensureIndexes((List<String>) daoProperties.get("uniqueIndexes"), true);
        ensureIndexes((List<String>) daoProperties.get("nonUniqueIndexes"), false);
        this.optimisticLockEnabled = Boolean.parseBoolean((String) daoProperties.get("optimisticLockEnabled"));
        this.showQueries = Boolean.parseBoolean((String) daoProperties.get("showQueries"));
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(String primaryKey)
            throws NoSuchItemException {
        try {
            List<Condition> conditions = new ArrayList<Condition>(2);
            conditions.add(buildIdCondition(primaryKey));

            QueryModel queryModel = new QueryModel();
            queryModel.setConditions(conditions);

            return findUniqueUsingQueryModel(queryModel);
        } catch (IllegalArgumentException e) {
            throw new NoSuchItemException(getCollectionClass().getSimpleName(), primaryKey);
        }
    }


    @Override
    public final Iterable<T> findAll() {
        return findUsingQueryModel(new QueryModel());
    }


    @Override
    public final void save(T entity) {
        saveAndReturn(entity);
    }


    @Override
    public final void delete(T entity) {
        DBObject dbo = (DBObject) enhancer.enhance(entity);

        DBObject identifier = createPrimaryIdentifyingQuery(dbo);

        deleteByIdentifier(identifier);
    }


    @Override
    public final void delete(String primaryKey) {
        deleteByIdentifier(createPrimaryIdentifyingQuery(primaryKey));
    }


    @Override
    public final void flush() {
        if (MongoDBSession.isActive()) {
            MongoDBSession.flush(this);
        }
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException {
        // Need to revisit te way caching works as it will miss some items...focus only on the identifier
        // instead of secondary cache keys...
        MongoDBCommand command = buildCommand(queryModel);

        if (MongoDBSession.isActive()) {
            // noinspection unchecked
            T cached = (T) MongoDBSession.getObjectFromCache(dbCollection.getName(), command.getQuery());

            if (cached != null) {
                DBObject identifier = createPrimaryIdentifyingQuery((DBObject) cached);

                MongoDBSession.trackForSave(this, identifier, cached,
                                            createIdentifyingQueries((DBObject) cached));

                return cached;
            }
        }

        // noinspection unchecked
        T result = (T) command.singleResult(dbCollection);

        if (MongoDBSession.isActive()) {
            DBObject identifier = createPrimaryIdentifyingQuery((DBObject) result);
            MongoDBSession.trackForSave(MongoDBQueryModelDAO.this, identifier, result,
                                        createIdentifyingQueries((DBObject) result));
        }

        return result;
    }


    public Iterable<T> findUsingQueryModel(QueryModel queryModel) {
        MongoDBCommand command = buildCommand(queryModel);
        DBCursor dbCursor = command.cursor(dbCollection);

        if (queryModel.getSorts() != null) {
            dbCursor.sort(processSorts(queryModel.getSorts()));
        }

        if (queryModel.getFirstResult() > 0) {
            dbCursor = dbCursor.skip(queryModel.getFirstResult());  // dbCursor is zero-indexed, firstResult is one-indexed
        }

        if (queryModel.getMaxResults() > 0) {
            dbCursor = dbCursor.limit(queryModel.getMaxResults());
        }

        final DBCursor finalDbCursor = dbCursor;

        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return finalDbCursor.hasNext();
                    }


                    @Override
                    @SuppressWarnings( { "unchecked" })
                    public T next() {
                        DBObject raw = finalDbCursor.next();
                        T t = enhancer.enhance((T) raw);

                        ((Dirtyable) t).markCurrentAsClean();

                        if (MongoDBSession.isActive()) {
                            MongoDBSession.trackForSave(MongoDBQueryModelDAO.this,
                                                        createPrimaryIdentifyingQuery(raw),
                                                        t,
                                                        createIdentifyingQueries(raw));
                        }

                        return t;
                    }


                    @Override
                    public void remove() {
                        finalDbCursor.remove();
                    }
                };
            }
        };
    }


    public Object projectUsingQueryModel(QueryModel queryModel) {
        try {
            return buildCommand(queryModel).singleResult(dbCollection);
        } catch (NoSuchItemException e) {
            return null;  // TODO: evaluate if correct
        }
    }


    @Override
    public Condition buildCondition(String conditionField,
                                    ConditionType conditionType,
                                    Iterator argsIterator) {
        MongoDBOperator mongoDBOperator = CONDITION_TYPE_TO_MONGO_OPERATOR.get(conditionType);

        if (mongoDBOperator == null) {
            throw new IllegalArgumentException("Unknown mapping for: " + conditionType);
        }

        if (conditionField.equals("id")) {
            return buildIdCondition(argsIterator.next());
        } else {
            return new Condition(conditionField, mongoDBOperator.buildConstraint(argsIterator));
        }
    }


    @Override
    public Projection buildProjection(String projectionField,
                                      ProjectionType projectionType,
                                      Iterator argsIterator) {
        return new Projection(projectionField, projectionType);
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected final T saveAndReturn(T entity) {
        T enhanced = enhancer.enhance(entity);
        DBObject dbo = (DBObject) enhanced;

        if (dbo.get(ID_FIELD) == null) {
            dbo.put(ID_FIELD, findOrCreateIdFor(dbo));
        }

        DBObject identifier = createPrimaryIdentifyingQuery(dbo);

        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForSave(this, identifier, enhanced, createIdentifyingQueries(dbo));
        } else {
            trueSave(identifier, dbo);
        }

        return enhanced;
    }


//    protected QueryModel augmentQueryModel(QueryModel queryModel) {
//        return ;
//    }


    /**
     * This method allows subclasses an opportunity to add any important data to a cache key, such
     * as the __acl from the AccessControlMongoDAO.
     * @param key key to augment
     * @return augmented key
     */
    protected DBObject augmentObjectCacheKey(DBObject key) {
        return key;
    }


    protected DBObject createPrimaryIdentifyingQuery(DBObject dbObject) {
        return createPrimaryIdentifyingQuery((ObjectId) dbObject.get("_id"));
    }


    protected DBObject[] createIdentifyingQueries(DBObject dbObject) {
        int uniqueIndexCount = uniqueIndexes.size() + 1;
        List<DBObject> queries = new ArrayList<DBObject>(uniqueIndexCount);

        queries.add(createPrimaryIdentifyingQuery(dbObject));

        for (Collection<String> indexFields : uniqueIndexes.values()) {
            DBObject query = new BasicDBObject();

            for (String indexField : indexFields) {
                query.put(indexField, getFieldValueFrom(dbObject, indexField));
            }

            queries.add(augmentObjectCacheKey(query));
        }

        return queries.toArray(new DBObject[uniqueIndexCount]);
    }


    //-------------------------------------------------------------
    // Methods - Protected - Final
    //-------------------------------------------------------------

    protected final DBObject createPrimaryIdentifyingQuery(String primaryKey) {
        if (primaryKey == null) {
            throw new IllegalArgumentException("Primary key cannot be null.");
        } else if (ObjectId.isValid(primaryKey)) {
            return createPrimaryIdentifyingQuery(new ObjectId(primaryKey));
        } else {
            return new BasicDBObject("_id", primaryKey);
        }
    }


    protected final DBObject createPrimaryIdentifyingQuery(ObjectId primaryKey) {
        return new BasicDBObject("_id", primaryKey);
    }


    protected final void update(QueryModel queryModel, DBObject update) {
        final DBObject query = buildQueryObject(queryModel);
        DBObject dbObject = (DBObject) DBObjectUtil.toDBObject(DBObject.class, update);

        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForUpdate(this, query, dbObject);
        } else {
            trueUpdate(query, dbObject);
        }
    }


    protected final void upsert(QueryModel queryModel, DBObject upsert) {
        final DBObject query = buildQueryObject(queryModel);

        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForUpsert(this, query, upsert);
        } else {
            trueUpsert(query, upsert);
        }
    }


    protected final void deleteByIdentifier(DBObject identifier) {
        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForDelete(this, identifier);
        } else {
            trueRemove(identifier);
        }
    }


    protected final void trueUpsert(final DBObject query, final DBObject upsert) {
        executeAndCheckLastError(new Runnable() {
            @Override
            public void run() {
                dbCollection.update(query, (DBObject) DBObjectUtil.toDBObject(DBObject.class, upsert), true, true);
            }
        });
    }


    protected final void trueUpdate(final DBObject identifier, final DBObject dbo) {
        executeAndCheckLastError(new Runnable() {
            @Override
            public void run() {
                // included this logic here just in case we want to enable
                // retry logic inside of 'executeAndCheckLastError' (it would
                // just need to re-run the runnable)
                if (optimisticLockEnabled) {
                    Integer optimisticLockVersion = (Integer) dbo.get(OPTIMISTIC_LOCK_VERSION_FIELD);

                    if (optimisticLockVersion == null) {
                        dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, 1);
                    } else {
                        identifier.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersion);

                        dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersion + 1);
                    }
                }

                if (showQueries) {
                    queryLogger.debug("Updating {} ({}) with query {}",
                                      new Object[] { getCollectionClass().getSimpleName(),
                                                     dbo.toMap(),
                                                     identifier.toMap() } );
                }

                dbCollection.update(identifier, dbo, false, true);
            }
        });
    }


    protected final void trueSave(final DBObject identifier, final DBObject dbo) {
        executeAndCheckLastError(new Runnable() {
            @Override
            public void run() {
                // included this logic here just in case we want to enable
                // retry logic inside of 'executeAndCheckLastError' (it would
                // just need to re-run the runnable)
                if (optimisticLockEnabled) {
                    Integer optimisticLockVersion = (Integer) dbo.get(OPTIMISTIC_LOCK_VERSION_FIELD);

                    if (optimisticLockVersion == null) {
                        dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, 1);
                    } else {
                        identifier.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersion);

                        dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersion + 1);
                    }
                }

                if (showQueries) {
                    queryLogger.debug("Saving new {} ({}) with query {}",
                                      new Object[] { getCollectionClass().getSimpleName(),
                                                     dbo.toMap(),
                                                     identifier.toMap() } );
                }

                dbCollection.update(identifier, dbo, true, false);
            }
        });
    }


    protected final void trueRemove(final DBObject identifier) {
        if (optimisticLockEnabled) {
            // TODO:
        }

        executeAndCheckLastError(new Runnable() {
            @Override
            public void run() {

                if (showQueries) {
                    queryLogger.debug("Removing {}s matching {}",
                                      new Object[] { getCollectionClass().getSimpleName(),
                                                     identifier.toMap() } );
                }

                dbCollection.remove(identifier);
            }
        });
    }


    protected final DBCollection getDbCollection() {
        return dbCollection;
    }


    protected final Enhancer<T> getEnhancer() {
        return enhancer;
    }


    protected final Class<?> getCollectionClass() {
        return enhancer.getBaseClass();
    }


    /**
     * Executes the given command inside a try-finally block that guarantees
     * any errors provided by the server will propagate as a RuntimeException.
     *
     * @param command Runnable that embeds a db command to execute
     */
    protected final void executeAndCheckLastError(final Runnable command) {
        executeAndCheckLastError(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                command.run();

                return null;
            }
        });
    }


    /**
     * Executes the given command inside a try-finally block that guarantees
     * any errors provided by the server will propagate as a RuntimeException.
     *
     * @param command Callable that embeds a db command to execute and then validate
     *
     * @return The result of the passed in command.
     */
    protected final <R> R executeAndCheckLastError(Callable<R> command) {
        db.requestStart();

        try {
            db.resetError();

            R result = command.call();

            checkLastError();

            return result;
        } catch (Exception e) {
            throw ExceptionUtil.propagate(e);
        } finally {
            db.requestDone();
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private DBObject processSorts(List<Sort> sorts) {
        DBObject orderBy = new BasicDBObject();

        for (Sort sort : sorts) {
            orderBy.put(sort.getField(), sort.getSortDirection() == SortDirection.Ascending ? 1 : -1);
        }

        return orderBy;
    }


    // Special case for 'id' queries as it maps to _id within MongoDB.
     private Condition buildIdCondition(Object argument) {
         if (String.class.isAssignableFrom(argument.getClass())) {
             return new Condition("_id", new ObjectId((String) argument));
         } else if (Iterable.class.isAssignableFrom(argument.getClass())) {
             List<ObjectId> objectIds = new ArrayList<ObjectId>();

             for (Object argumentItem : (Iterable<?>) argument) {
                 if (argumentItem == null) {
                     objectIds.add(null);
                 } else if (argumentItem instanceof ObjectId) {
                     objectIds.add((ObjectId) argumentItem);
                 } else if (argumentItem instanceof String) {
                     objectIds.add(new ObjectId((String) argumentItem));
                 } else {
                     throw new IllegalArgumentException("Don't know how to handle class for 'id' mapping: " + argumentItem.getClass());
                 }
             }

             // TODO: create getter for MongoDBOperator.operator() ...?
             return new Condition("_id", new BasicDBObject("$in", objectIds));
         } else if (argument instanceof ObjectId) {
             return new Condition("_id", argument);
         } else {
             throw new IllegalArgumentException("Don't know how to handle class for 'id' mapping: "
                                                + argument.getClass());
         }
     }


     private ObjectId findOrCreateIdFor(DBObject dbo) {
        if (MongoDBSession.isActive()) {
            for (DBObject key : createIdentifyingQueries(dbo)) {
                if (key == null) {
                    continue;
                }
                
                Object cached = MongoDBSession.getObjectFromCache(getDbCollection().getName(), key);
                if (cached instanceof DBObject) {
                    if (((DBObject) cached).get(ID_FIELD) != null) {
                        return (ObjectId) ((DBObject) cached).get(ID_FIELD);
                    }
                }
            }
        }

        return new ObjectId();
    }


    private Object getFieldValueFrom(DBObject dbo, String field) {
        if (!field.contains(".")) {
            return dbo.get(field);
        }

        Object value = dbo;

        for (String subField : field.split(".")) {
            value = ((DBObject) value).get(subField);

            if (value == null) {
                break;
            }
        }

        return value;
    }


    private MongoDBCommand buildCommand(QueryModel queryModel) {
        BasicDBObject query = buildQueryObject(queryModel);
        MongoDBCommand command;

        if (queryModel.getProjection() == null) {
            command = new BasicDBObjectCommand(query, enhancer);
        } else {
            command = ProjectionCommands.forProjection(queryModel.getProjection(), query);
        }

        if (showQueries) {
            return QueryLoggingCommand.wrap(command, queryLogger);
        } else {
            return command;
        }
    }


    private Map<String, Set<String>> ensureIndexes(List<String> uniqueIndexes, final boolean unique) {
        if (uniqueIndexes == null || uniqueIndexes.size() == 0) {
            return Collections.emptyMap();
        }

        Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        for (final String uniqueIndex : uniqueIndexes) {
            final DBObject index = new BasicDBObject();
            String[] indexFields = uniqueIndex.split(",");

            for (String indexField : indexFields) {
                if (indexField.startsWith("+")) {
                    index.put(indexField.substring(1), 1);
                } else if (indexField.startsWith("-")) {
                    index.put(indexField.substring(1), -1);
                } else {
                    index.put(indexField, 1);
                }
            }

            result.put(uniqueIndex, index.keySet());

            executeAndCheckLastError(new Runnable() {
                @Override
                public void run() {
                    if (showQueries) {
                        queryLogger.debug("Ensuring index {} on {}",
                                          new Object[] { index.toMap(),
                                                         getCollectionClass().getSimpleName() } );
                    }

                    dbCollection.ensureIndex(index, createIndexName(uniqueIndex), unique);
                }
            });
        }

        return result;
    }


    private String createIndexName(String indexSpec) {
        return indexSpec.replace(',', '-');
    }


    private BasicDBObject buildQueryObject(QueryModel queryModel) {
        BasicDBObject query = new BasicDBObject();

        // TODO: add "augmentConditions() support back in....

        List<Condition> allCriteria = new ArrayList<Condition>();

        if (queryModel.getConditions() != null) {
            allCriteria.addAll(queryModel.getConditions());
        }

        for (Map.Entry<String, List<Condition>> associationConditions : queryModel.getAssociationConditions().entrySet()) {
            for (Condition condition : associationConditions.getValue()) {
                condition.setField(associationConditions.getKey() + "." + condition.getField());

                allCriteria.add(condition);
            }
        }

        // TODO: optimize this -- iterating over everything above and again here
        for (Condition condition : allCriteria) {
            // we need to ensure that all condition objects are mongodb-safe
            // examples of "unsafe" things are: Sets, Enum, POJOs.
            Object rawConstraint = condition.getConstraint();
            Object constraint = (rawConstraint == null) ? null
                                                        : DBObjectUtil.toDBObject(rawConstraint.getClass(), rawConstraint);

            // XXX : if annotation specifies multiple conditions on single field the
            // first condition will be overwritten here
            query.put(condition.getField(), constraint);
        }

        return query;
    }


    /**
     * Checks for errors. An example error, returned when a uniqueness constraint is violated, is:
     * <code>
     * {err=E11000 duplicate key error index: 4f78-aa0b-f805692e8356--unittest.SimpleObject.$intValue_1 dup key: { : 3 },
     *  code=11000,
     *  n=0,
     *  ok=1.0,
     *  _ns=$cmd}
     * </code>
     * When everything works fine, then we'll see something like:
     * <code>
     * {err=null, updatedExisting=false, n=1, ok=1.0, _ns=$cmd}
     * </code>
     */
    private void checkLastError() {
        MongoDBError lastError = getLastError();

        if (lastError != null) {
            String err = lastError.getErr();
            int code = lastError.getCode();

            switch (code) {
                case MongoDBError.NO_CODE:
                    throw new MongoDBRuntimeException(err);
                case 10055: // fall through
                case 10059:
                    throw new MongoDBObjectTooLargeRuntimeException(err);
                case 11000: // fall through
                case 11001:
                    throw new UniquenessViolationRuntimeException(err);
                default:
                    throw new MongoDBRuntimeException(code, err);
            }
        }
    }


    private MongoDBError getLastError() {
        DBObject lastError = db.getLastError();

        if (lastError == null || lastError.get("err") == null) {
            return null;
        }

        MongoDBError err = ERROR_ENHANCER.newInstance();
        ((DBObject) err).putAll(lastError);

        return err;
    }
}
