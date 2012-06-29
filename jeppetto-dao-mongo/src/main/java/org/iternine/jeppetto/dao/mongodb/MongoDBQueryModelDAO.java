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

package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.AccessControlContext;
import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.AccessControlException;
import org.iternine.jeppetto.dao.AccessControlDAO;
import org.iternine.jeppetto.dao.AccessType;
import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.ConditionType;
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
import org.iternine.jeppetto.dao.annotation.AccessControl;
import org.iternine.jeppetto.dao.annotation.Accessor;
import org.iternine.jeppetto.dao.annotation.Creator;
import org.iternine.jeppetto.dao.mongodb.enhance.DBObjectUtil;
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObject;
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObjectList;
import org.iternine.jeppetto.dao.mongodb.enhance.DirtyableDBObjectMap;
import org.iternine.jeppetto.dao.mongodb.enhance.EnhancerHelper;
import org.iternine.jeppetto.dao.mongodb.enhance.MongoDBDecoder;
import org.iternine.jeppetto.dao.mongodb.projections.ProjectionCommands;
import org.iternine.jeppetto.enhance.Enhancer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBDecoder;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
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
import java.util.regex.Pattern;


/**
 * Provides a QueryModelDAO implementation for Mongo.
 * <p/>
 * This QueryModelDAO implementation for MongoDB provides bi-directional ODM (Object to Document Mapping) as well
 * as the rich query capabilities found in the core org.iternine.jeppetto.dao package.  It requires that the ID be a String.
 *
 * Instantiation requires a daoProperties Map<String, Object> that will look for the following keys (and expected
 * values):
 * <p/>
 * <table>
 *   <tr>
 *     <td>Key</td>
 *     <td>Required?</td>
 *     <td>Description</td>
 *   </tr>
 *   <tr>
 *     <td>db</td>
 *     <td>Yes</td>
 *     <td>Instance of a configured com.mongodb.DB</td>
 *   </tr>
 *   <tr>
 *     <td>uniqueIndexes</td>
 *     <td>No</td>
 *     <td>List<String> of various MongoDB index values that will be ensured to exist and must be unique.</td>
 *   </tr>
 *   <tr>
 *     <td>nonUniqueIndexes</td>
 *     <td>No</td>
 *     <td>List<String> of various MongoDB index values that will be ensured to exist and need not be unique.</td>
 *   </tr>
 *   <tr>
 *     <td>optimisticLockEnabled</td>
 *     <td>No</td>
 *     <td>Boolean to indicate if instances of the tracked type should be protected by a Jeppetto-managed lock version field.</td>
 *   </tr>
 *   <tr>
 *     <td>shardKeyPattern</td>
 *     <td>No</td>
 *     <td>A comma-separated string of fields that are used to determine the shard key(s) for the collection.</td>
 *   </tr>
 *   <tr>
 *     <td>saveNulls</td>
 *     <td>No</td>
 *     <td>Boolean to indicate whether null fields should be included in MongoDB documents.</td>
 *   </tr>
 *   <tr>
 *     <td>writeConcern</td>
 *     <td>No</td>
 *     <td>String, one of the values as indicated at http://www.mongodb.org/display/DOCS/Replica+Set+Semantics.  If
 *         not specified, the DAO defaults to "SAFE".</td>
 *   </tr>
 *   <tr>
 *     <td>showQueries</td>
 *     <td>No</td>
 *     <td>Boolean to indicate if executed queries should be logged.  Note that logging will need to be enabled for
 *         the DAO's package as well.</td>
 *   </tr>
 * </table>
 * @param <T> the type of persistent object this DAO will manage.
 */
// TODO: support createdDate/lastModifiedDate (createdDate from get("_id").getTime()?)
// TODO: Implement determineOptimalDBObject
//          - understand saveNulls
//          - understand field-level deltas
// TODO: support per-call WriteConcerns (keep in mind session semantics)
// TODO: investigate usage of ClassLoader so new instances are already enhanced
public class MongoDBQueryModelDAO<T, ID>
        implements QueryModelDAO<T, ID>, AccessControlDAO<T, ID> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final String ID_FIELD = "_id";
    private static final String OPTIMISTIC_LOCK_VERSION_FIELD = "__olv";
    private static final String ACCESS_CONTROL_FIELD = "__acl";
    private static final Pattern READ_PATTERN = Pattern.compile("^R");


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private DBCollection dbCollection;
    private Enhancer<T> enhancer;
    private AccessControlContextProvider accessControlContextProvider;
    private Map<String, Set<String>> uniqueIndexes;
    private boolean optimisticLockEnabled;
    private List<String> shardKeys;
    private boolean saveNulls;
    private WriteConcern defaultWriteConcern;
    private Logger queryLogger;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected MongoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties) {
        this(entityClass, daoProperties, null);
    }


    @SuppressWarnings( { "unchecked" })
    protected MongoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties,
                                   AccessControlContextProvider accessControlContextProvider) {
        this.dbCollection = ((DB) daoProperties.get("db")).getCollection(entityClass.getSimpleName());
        this.enhancer = EnhancerHelper.getDirtyableDBObjectEnhancer(entityClass);

        dbCollection.setObjectClass(enhancer.getEnhancedClass());
        dbCollection.setDBDecoderFactory(new DBDecoderFactory() {
            @Override
            public DBDecoder create() {
                return new MongoDBDecoder();
            }
        });

        this.accessControlContextProvider = accessControlContextProvider;
        this.uniqueIndexes = ensureIndexes((List<String>) daoProperties.get("uniqueIndexes"), true);
        ensureIndexes((List<String>) daoProperties.get("nonUniqueIndexes"), false);
        this.optimisticLockEnabled = Boolean.parseBoolean((String) daoProperties.get("optimisticLockEnabled"));
        this.shardKeys = extractShardKeys((String) daoProperties.get("shardKeyPattern"));
        this.saveNulls = Boolean.parseBoolean((String) daoProperties.get("saveNulls"));

        if (daoProperties.containsKey("writeConcern")) {
            this.defaultWriteConcern = WriteConcern.valueOf((String) daoProperties.get("writeConcern"));
        } else {
            this.defaultWriteConcern = WriteConcern.SAFE;
        }

        if (Boolean.parseBoolean((String) daoProperties.get("showQueries"))) {
            queryLogger = LoggerFactory.getLogger(getClass());
        }
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(ID id)
            throws NoSuchItemException, JeppettoException {
        try {
            QueryModel queryModel = new QueryModel();
            queryModel.addCondition(buildIdCondition(id));

            if (accessControlContextProvider != null) {
                queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
            }

            return findUniqueUsingQueryModel(queryModel);
        } catch (IllegalArgumentException e) {
            throw new NoSuchItemException(getCollectionClass().getSimpleName(), id.toString());
        }
    }


    @Override
    public final Iterable<T> findAll()
            throws JeppettoException {
        QueryModel queryModel = new QueryModel();

        if (accessControlContextProvider != null) {
            queryModel.setAccessControlContext(accessControlContextProvider.getCurrent());
        }

        return findUsingQueryModel(queryModel);
    }


    @Override
    public final void save(T entity)
            throws OptimisticLockException, JeppettoException {
        T enhancedEntity = enhancer.enhance(entity);
        DirtyableDBObject dbo = (DirtyableDBObject) enhancedEntity;

        if (dbo.isPersisted()) {
            if (accessControlContextProvider != null) {
                verifyWriteAllowed(dbo, accessControlContextProvider.getCurrent()); // TODO: should access control check be added to identifyingQuery?
            }
        } else {
            if (dbo.get(ID_FIELD) == null) {
                dbo.put(ID_FIELD, new ObjectId());  // If the id isn't explicitly set, assume intent is for mongo ids
            }

            if (accessControlContextProvider != null) {
                assessAndAssignAccessControl(dbo, accessControlContextProvider.getCurrent());
            }
        }

        DBObject identifyingQuery = buildIdentifyingQuery(dbo);

        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForSave(this, identifyingQuery, enhancedEntity, createIdentifyingQueries(dbo));
        } else {
            trueSave(identifyingQuery, dbo);
        }
    }


    @Override
    public final void delete(T entity)
            throws JeppettoException {
        // TODO: Probably don't want to enhance this object as we may need a previously retrieved object so
        // we can construct an appropriate identifying query w/ __olv
        DBObject dbo = (DBObject) enhancer.enhance(entity);

        DBObject identifyingQuery = buildIdentifyingQuery(dbo);

        for (String shardKey : shardKeys) {
            identifyingQuery.put(shardKey, dbo.get(shardKey));
        }

        deleteByIdentifyingQuery(identifyingQuery);
    }


    @Override
    public final void deleteById(ID id)
            throws JeppettoException {
        deleteByIdentifyingQuery(buildIdentifyingQuery(id));
    }


    @Override
    public final void flush()
            throws JeppettoException {
        if (MongoDBSession.isActive()) {
            MongoDBSession.flush(this);
        }
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException, TooManyItemsException, JeppettoException {
        // Need to revisit te way caching works as it will miss some items...focus only on the identifyingQuery
        // instead of secondary cache keys...
        MongoDBCommand command = buildCommand(queryModel, AccessType.Read);

        if (MongoDBSession.isActive()) {
            // noinspection unchecked
            T cached = (T) MongoDBSession.getObjectFromCache(dbCollection.getName(), command.getQuery());

            if (cached != null) {
                DBObject identifyingQuery = buildIdentifyingQuery((DBObject) cached);

                MongoDBSession.trackForSave(this, identifyingQuery, cached, createIdentifyingQueries((DBObject) cached));

                return cached;
            }
        }

        // noinspection unchecked
        T result = (T) command.singleResult(dbCollection);

        if (MongoDBSession.isActive()) {
            DBObject identifyingQuery = buildIdentifyingQuery((DBObject) result);

            MongoDBSession.trackForSave(this, identifyingQuery, result, createIdentifyingQueries((DBObject) result));
        }

        return result;
    }


    public Iterable<T> findUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        MongoDBCommand command = buildCommand(queryModel, AccessType.Read);
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
                        DBObject result = finalDbCursor.next();

                        ((DirtyableDBObject) result).markPersisted();

                        if (MongoDBSession.isActive()) {
                            MongoDBSession.trackForSave(MongoDBQueryModelDAO.this,
                                                        buildIdentifyingQuery(result),
                                                        (T) result,
                                                        createIdentifyingQueries(result));
                        }

                        return (T) result;
                    }


                    @Override
                    public void remove() {
                        finalDbCursor.remove();
                    }
                };
            }
        };
    }


    public Object projectUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        try {
            return buildCommand(queryModel, AccessType.Read).singleResult(dbCollection);
        } catch (NoSuchItemException e) {
            return null;  // TODO: evaluate if correct
        }
    }


    @Override
    public void deleteUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        try {
            DBObject deleteQuery = buildQueryObject(queryModel, AccessType.ReadWrite);

            if (queryLogger != null) {
                queryLogger.debug("Deleting {}s identified by {}",
                                  new Object[] { getCollectionClass().getSimpleName(), deleteQuery.toMap() } );
            }

            dbCollection.remove(deleteQuery, getWriteConcern());
        } catch (MongoException e) {
            throw new JeppettoException(e);
        }
    }


    @Override
    public Condition buildCondition(String conditionField,
                                    ConditionType conditionType,
                                    Iterator argsIterator) {
        if (conditionField.equals("id")) {
            return buildIdCondition(argsIterator.next());
        } else {
            return new Condition(conditionField,
                                 MongoDBOperator.valueOf(conditionType.name()).buildConstraint(argsIterator));
        }
    }


    @Override
    public Projection buildProjection(String projectionField,
                                      ProjectionType projectionType,
                                      Iterator argsIterator) {
        return new Projection(projectionField, projectionType);
    }


    //-------------------------------------------------------------
    // Implementation - AccessControlDAO
    //-------------------------------------------------------------

    @Override
    public void save(T object, AccessControlContext accessControlContext)
            throws OptimisticLockException, AccessControlException, JeppettoException {
        ensureAccessControlEnabled();

        T enhancedEntity = enhancer.enhance(object);
        DirtyableDBObject dbo = (DirtyableDBObject) enhancedEntity;

        if (dbo.isPersisted()) {
            verifyWriteAllowed(dbo, accessControlContext); // TODO: should access control check be added to identifyingQuery?
        } else {
            if (dbo.get(ID_FIELD) == null) {
                dbo.put(ID_FIELD, new ObjectId());  // If the id isn't explicitly set, assume intent is for mongo ids
            }

            assessAndAssignAccessControl(dbo, accessControlContext);
        }

        DBObject identifyingQuery = buildIdentifyingQuery(dbo);

        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForSave(this, identifyingQuery, enhancedEntity, createIdentifyingQueries(dbo));
        } else {
            trueSave(identifyingQuery, dbo);
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

        QueryModel queryModel = new QueryModel();
        queryModel.addCondition(buildIdCondition(id));
        queryModel.setAccessControlContext(accessControlContext);

        DBObject dbo = (DBObject) findUniqueUsingQueryModel(queryModel);

        verifyWriteAllowed(dbo, accessControlContext);

        @SuppressWarnings( { "unchecked" })
        Map<String, String> accessControl = (Map<String, String>) dbo.get(ACCESS_CONTROL_FIELD);

        DBObject accessUpdate;

        if (accessControl == null) {
            accessUpdate = new BasicDBObject("$set", new BasicDBObject(ACCESS_CONTROL_FIELD,
                                                                       new BasicDBObject(accessId, accessType.shortName())));
        } else {
            accessUpdate = new BasicDBObject("$set", new BasicDBObject(ACCESS_CONTROL_FIELD + "." + accessId,
                                                                       accessType.shortName()));
        }

        DBObject identifyingQuery = buildIdentifyingQuery(dbo);

        for (String shardKey : shardKeys) {
            identifyingQuery.put(shardKey, dbo.get(shardKey));
        }

        if (queryLogger != null) {
            queryLogger.info("Granting access to object identified by {} to {}.", identifyingQuery.toMap(), accessId);
        }

        try {
            dbCollection.update(identifyingQuery, accessUpdate, true, false, getWriteConcern());
        } catch (MongoException e) {
            throw new JeppettoException(e);
        }
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

        QueryModel queryModel = new QueryModel();
        queryModel.addCondition(buildIdCondition(id));
        queryModel.setAccessControlContext(accessControlContext);

        DBObject dbo = (DBObject) findUniqueUsingQueryModel(queryModel);

        verifyWriteAllowed(dbo, accessControlContext);

        @SuppressWarnings("unchecked")
        Map<String, String> accessControl = (Map<String, String>)  dbo.get(ACCESS_CONTROL_FIELD);

        if (accessControl == null) {
            return;
        }

        DBObject accessUpdate = new BasicDBObject("$pull", new BasicDBObject(ACCESS_CONTROL_FIELD, accessId));
        DBObject identifyingQuery = buildIdentifyingQuery(dbo);

        for (String shardKey : shardKeys) {
            identifyingQuery.put(shardKey, dbo.get(shardKey));
        }

        if (queryLogger != null) {
            // TODO: What happens if revoke happens and another thread already has object in memory and tries to save()?
            queryLogger.info("Revoking access to object identified by {} to {}.", identifyingQuery.toMap(), accessId);
        }

        try {
            dbCollection.update(identifyingQuery, accessUpdate, true, false, getWriteConcern());
        } catch (MongoException e) {
            throw new JeppettoException(e);
        }
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

        QueryModel queryModel = new QueryModel();
        queryModel.addCondition(buildIdCondition(id));
        queryModel.setAccessControlContext(accessControlContext);

        DBObject dbo = (DBObject) findUniqueUsingQueryModel(queryModel);

        // We limit it to writers.
        verifyWriteAllowed(dbo, accessControlContext);

        @SuppressWarnings("unchecked")
        Map<String, String> accessControl = (Map<String, String>) dbo.get(ACCESS_CONTROL_FIELD);

        if (accessControl == null || accessControl.size() == 0) {
            return Collections.emptyMap();
        } else if (accessControl.size() == 1) {
            Map.Entry<String, String> entry = accessControl.entrySet().iterator().next();

            return Collections.singletonMap(entry.getKey(), AccessType.getAccessTypeFromShortName(entry.getValue()));
        } else {
            Map<String, AccessType> result = new HashMap<String, AccessType>();

            for (Map.Entry<String, String> entry : accessControl.entrySet()) {
                result.put(entry.getKey(), AccessType.getAccessTypeFromShortName(entry.getValue()));
            }

            return result;
        }
    }


    @Override
    public AccessControlContextProvider getAccessControlContextProvider() {
        return accessControlContextProvider;
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    /**
     * This method allows subclasses an opportunity to add any important data to a cache key, such
     * as the __acl from the AccessControlMongoDAO.
     *
     * TODO: revsit these methods...
     *
     * @param key key to augment
     * @return augmented key
     */
    protected DBObject augmentObjectCacheKey(DBObject key) {
        return key;
    }


    protected DBObject[] createIdentifyingQueries(DBObject dbo) {
        int uniqueIndexCount = uniqueIndexes.size() + 1;
        List<DBObject> queries = new ArrayList<DBObject>(uniqueIndexCount);

        queries.add(buildIdentifyingQuery(dbo));

        for (Collection<String> indexFields : uniqueIndexes.values()) {
            DBObject query = new BasicDBObject();

            for (String indexField : indexFields) {
                query.put(indexField, getFieldValueFrom(dbo, indexField));
            }

            queries.add(augmentObjectCacheKey(query));
        }

        return queries.toArray(new DBObject[uniqueIndexCount]);
    }


    //-------------------------------------------------------------
    // Methods - Protected - Final
    //-------------------------------------------------------------

    protected final DBObject buildIdentifyingQuery(DBObject dbo) {
        //noinspection unchecked
        return buildIdentifyingQuery((ID) dbo.get(ID_FIELD));
    }


    protected final DBObject buildIdentifyingQuery(ID id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null.");
        } else if (id instanceof String && ObjectId.isValid((String) id)) {
            return new BasicDBObject(ID_FIELD, new ObjectId((String) id));
        } else {
            return new BasicDBObject(ID_FIELD, id);
        }
    }


    protected final void deleteByIdentifyingQuery(DBObject identifyingQuery) {
        if (MongoDBSession.isActive()) {
            MongoDBSession.trackForDelete(this, identifyingQuery);
        } else {
            trueRemove(identifyingQuery);
        }
    }


    protected final void trueSave(final DBObject identifyingQuery, final DirtyableDBObject dbo) {
        if (optimisticLockEnabled) {
            Integer optimisticLockVersion = (Integer) dbo.get(OPTIMISTIC_LOCK_VERSION_FIELD);
            int optimisticLockVersionValue = optimisticLockVersion == null ? 0 : optimisticLockVersion;

            // TODO: should this modification of identifyingQuery been done earlier (in save())?
            identifyingQuery.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersionValue);

            dbo.put(OPTIMISTIC_LOCK_VERSION_FIELD, optimisticLockVersionValue + 1);
        }

        for (String shardKey : shardKeys) {
            identifyingQuery.put(shardKey, dbo.get(shardKey));
        }

        final DBObject optimalDbo = determineOptimalDBObject(dbo);

        if (optimalDbo.keySet().size() == 0) {
            if (queryLogger != null) {
                queryLogger.debug("Bypassing save on object identified by {}; optimization rendered no changes.",
                                  identifyingQuery.toMap());
            }

            return;
        }

        if (queryLogger != null) {
            queryLogger.debug("Saving {} identified by {} with document {}",
                              new Object[] { getCollectionClass().getSimpleName(),
                                             identifyingQuery.toMap(),
                                             optimalDbo.toMap() } );
        }

        try {
            dbCollection.update(identifyingQuery, optimalDbo, true, false, getWriteConcern());
        } catch (MongoException.DuplicateKey e) {
            if (optimisticLockEnabled && e.getMessage().contains("$_id_")) {
                Integer localOptimisticLockVersion = (Integer) dbo.get(OPTIMISTIC_LOCK_VERSION_FIELD) - 1;

                if (localOptimisticLockVersion == 0) {
                    throw new JeppettoException(e); // TODO: id reuse...
                }

                identifyingQuery.removeField(OPTIMISTIC_LOCK_VERSION_FIELD);
                DBObject result = dbCollection.findOne(identifyingQuery);

                if (result == null) {
                    throw new OptimisticLockException("Probably an OptimisticLockException, but conflicting object "
                                                      + "identified by " + identifyingQuery + " no longer exists.");
                }

                Integer remoteOptimisticLockVersion = (Integer) result.get(OPTIMISTIC_LOCK_VERSION_FIELD);

                if (remoteOptimisticLockVersion != null && remoteOptimisticLockVersion > localOptimisticLockVersion) {
                    throw new OptimisticLockException("Local version = " + localOptimisticLockVersion
                                                      + ", remote version = " + remoteOptimisticLockVersion);
                }
            }

            throw new JeppettoException(e);
        } catch (MongoException e) {
            throw new JeppettoException(e);
        }

        dbo.markPersisted();
    }


    protected final void trueRemove(final DBObject identifyingQuery) {
        if (optimisticLockEnabled) {
            // TODO:
        }

        if (queryLogger != null) {
            queryLogger.debug("Removing {}s matching {}",
                              new Object[] { getCollectionClass().getSimpleName(), identifyingQuery.toMap() } );
        }

        dbCollection.remove(identifyingQuery, getWriteConcern());
    }


    protected final DBCollection getDbCollection() {
        return dbCollection;
    }


    protected final Class<?> getCollectionClass() {
        return enhancer.getBaseClass();
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
         if (argument instanceof String && ObjectId.isValid((String) argument)) {
             return new Condition(ID_FIELD, new ObjectId((String) argument));
         } else if (Iterable.class.isAssignableFrom(argument.getClass())) {
             List<ObjectId> objectIds = new ArrayList<ObjectId>();

             //noinspection ConstantConditions
             for (Object argumentItem : (Iterable) argument) {
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
             return new Condition(ID_FIELD, new BasicDBObject("$in", objectIds));
         } else {
             return new Condition(ID_FIELD, argument);
         }
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


    private MongoDBCommand buildCommand(QueryModel queryModel, AccessType accessType) {
        BasicDBObject query = buildQueryObject(queryModel, accessType);
        MongoDBCommand command;

        if (queryModel.getProjection() == null) {
            command = new BasicDBObjectCommand(query);
        } else {
            command = ProjectionCommands.forProjection(queryModel.getProjection(), query);
        }

        if (queryLogger != null) {
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
                indexField = indexField.trim();

                if (indexField.startsWith("+")) {
                    index.put(indexField.substring(1), 1);
                } else if (indexField.startsWith("-")) {
                    index.put(indexField.substring(1), -1);
                } else {
                    index.put(indexField, 1);
                }
            }

            result.put(uniqueIndex, index.keySet());

            if (queryLogger != null) {
                queryLogger.debug("Ensuring index {} on {}",
                                  new Object[] { index.toMap(), getCollectionClass().getSimpleName() } );
            }

            dbCollection.ensureIndex(index, createIndexName(uniqueIndex), unique);
        }

        return result;
    }


    private String createIndexName(String indexSpec) {
        return indexSpec.replace(',', '-');
    }


    private List<String> extractShardKeys(String shardKeyPattern) {
        if (shardKeyPattern == null) {
            return Collections.emptyList();
        }

        String[] shardKeyParts = shardKeyPattern.split(",");
        List<String> shardKeys = new ArrayList<String>(shardKeyParts.length);

        for (String shardKey : shardKeyParts) {
            shardKey = shardKey.trim();

            if (shardKey.equals("id") || shardKey.equals("_id")) {
                continue;
            }

            shardKeys.add(shardKey);
        }

        return shardKeys;
    }


    private BasicDBObject buildQueryObject(QueryModel queryModel, AccessType accessType) {
        BasicDBObject query = new BasicDBObject();
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
                                                        : DBObjectUtil.toDBObject(rawConstraint);

            // XXX : if annotation specifies multiple conditions on single field the
            // first condition will be overwritten here
            query.put(condition.getField(), constraint);
        }

        if (accessControlContextProvider != null) {
            if (!annotationAllowsAccess(queryModel.getAccessControlContext(), accessType)) {
                if (accessType == AccessType.Read) {
                    query.put(ACCESS_CONTROL_FIELD + "." + queryModel.getAccessControlContext().getAccessId(), READ_PATTERN);
                } else {
                    query.put(ACCESS_CONTROL_FIELD + "." + queryModel.getAccessControlContext().getAccessId(), accessType.shortName());
                }
            }
        }

        return query;
    }


    private DBObject determineOptimalDBObject(DirtyableDBObject dirtyableDBObject) {
        if (!dirtyableDBObject.isPersisted()) {
//            dirtyableDBObject.includeNullValuedKeys(saveNulls);

            return dirtyableDBObject;
        }

        DBObject settableItems = new BasicDBObject();
        DBObject unsettableItems = new BasicDBObject();

        walkDirtyableDBObject("", dirtyableDBObject, settableItems, unsettableItems);

        if (optimisticLockEnabled) {
            // TODO: Don't like re-reading this value here, when handled in calling method
            settableItems.put(OPTIMISTIC_LOCK_VERSION_FIELD, dirtyableDBObject.get(OPTIMISTIC_LOCK_VERSION_FIELD));
        }

        DBObject optimalDBObject = new BasicDBObject();

        if (settableItems.keySet().size() > 0) {
            optimalDBObject.put("$set", settableItems);
        }

        if (unsettableItems.keySet().size() > 0) {
            optimalDBObject.put("$unset", unsettableItems);
        }

        return optimalDBObject;
    }


    private void walkDirtyableDBObject(String prefix, DirtyableDBObject dirtyableDBObject,
                                       DBObject settableItems, DBObject unsettableItems) {
        for (Iterator<String> dirtyKeys = dirtyableDBObject.getDirtyKeys(); dirtyKeys.hasNext(); ) {
            String dirtyKey = dirtyKeys.next();
            Object dirtyObject = dirtyableDBObject.get(dirtyKey);

            if (dirtyObject instanceof DirtyableDBObjectList) {   // NB: encompasses DirtyableDBObjectSet
                DirtyableDBObjectList dirtyableDBObjectList = (DirtyableDBObjectList) dirtyObject;

                if (!dirtyableDBObjectList.isPersisted() || dirtyableDBObjectList.isRewrite()) {
                    settableItems.put(prefix + dirtyKey, dirtyableDBObjectList);

                    continue;
                }

                walkDirtyableDBObject(prefix + dirtyKey + ".", dirtyableDBObjectList, settableItems, unsettableItems);
            } else if (dirtyObject instanceof DirtyableDBObjectMap) {
                DirtyableDBObjectMap dirtyableDBObjectMap = (DirtyableDBObjectMap) dirtyObject;

                if (!dirtyableDBObjectMap.isPersisted()) {
                    settableItems.put(prefix + dirtyKey, dirtyableDBObjectMap);

                    continue;
                }

                for (Object removedKey : dirtyableDBObjectMap.getRemovedKeys()) {
                    unsettableItems.put(prefix + dirtyKey + "." + removedKey, 1);
                }

                walkDirtyableDBObject(prefix + dirtyKey + ".", dirtyableDBObjectMap, settableItems, unsettableItems);
            } else if (dirtyObject instanceof DirtyableDBObject) {
                if (!((DirtyableDBObject) dirtyObject).isPersisted()) {
                    settableItems.put(prefix + dirtyKey, dirtyObject);
                } else {
                    walkDirtyableDBObject(prefix + dirtyKey + ".", (DirtyableDBObject) dirtyObject, settableItems, unsettableItems);
                }
            } else {
                settableItems.put(prefix + dirtyKey, DBObjectUtil.toDBObject(dirtyObject));
            }
        }
    }


    private WriteConcern getWriteConcern() {
        // TODO: Add ability to swap a concern out on a per-call basis...if nothing overwrites/changes, then
        // return the default.  Keep in mind session semantics (e.g. delayed saves).

        return defaultWriteConcern;
    }


    private void ensureAccessControlEnabled() {
        if (accessControlContextProvider == null) {
            throw new AccessControlException("Access Control is not enabled. No AccessControlContextProvider specified.");
        }
    }


    // TODO: ensure this is called by all update paths (e.g. other callers to trueSave())
    private void verifyWriteAllowed(DBObject dbo, AccessControlContext accessControlContext)
            throws AccessControlException {
        @SuppressWarnings( { "unchecked" })
        Map<String, String> accessControl = (Map<String, String>) dbo.get(ACCESS_CONTROL_FIELD);

        if (accessControlContext == null
            || (!AccessType.ReadWrite.shortName().equals(accessControl.get(accessControlContext.getAccessId()))
                && !annotationAllowsAccess(accessControlContext, AccessType.ReadWrite))) {
                throw new AccessControlException("Unable to write " + dbo.toMap() + " with " + accessControlContext);
        }
    }


    private void assessAndAssignAccessControl(DBObject dbo, AccessControlContext accessControlContext)
            throws AccessControlException {
        AccessControl accessControl = getAccessControlAnnotation();
        if (accessControl != null) {
            for (Creator creator : accessControl.creators()) {
                switch (creator.type()) {
                case Identified:
                    if (accessControlContext.getAccessId() != null) {
                        dbo.put(ACCESS_CONTROL_FIELD, Collections.singletonMap(accessControlContext.getAccessId(),
                                                                               creator.grantedAccess().shortName()));
                        return;
                    }

                    break;

                case Role:
                    if (accessControlContext.getRoles() != null && accessControlContext.getRoles().contains(creator.typeValue())) {
                        dbo.put(ACCESS_CONTROL_FIELD, Collections.singletonMap(accessControlContext.getAccessId(),
                                                                               creator.grantedAccess().shortName()));

                        return;
                    }

                    break;

                case Anonymous:
                    // No explicit grants given.
                    dbo.put(ACCESS_CONTROL_FIELD, Collections.emptyMap());

                    return;
                }
            }

            throw new AccessControlException("Unable to create " + dbo.getClass().getSuperclass().getSimpleName()
                                             + " with " + accessControlContext
                                             + ".  Check object's @AccessControl annotation.");
        } else {
            // When no annotation is present, any user can create the object.  If user is unknown, no explicit grants.
            // Otherwise, ReadWrite access is given to the caller.
            if (accessControlContext.getAccessId() == null) {
                dbo.put(ACCESS_CONTROL_FIELD, Collections.emptyMap());
            } else {
                dbo.put(ACCESS_CONTROL_FIELD, Collections.singletonMap(accessControlContext.getAccessId(),
                                                                       AccessType.ReadWrite.shortName()));
            }
        }
    }


    private boolean annotationAllowsAccess(AccessControlContext accessControlContext, AccessType accessType) {
        if (accessType == null) {
            return false;
        }

        AccessControl accessControl;
        if ((accessControl = getAccessControlAnnotation()) == null) {
            return false;
        }

        Set<String> roles = accessControlContext == null ? Collections.<String>emptySet() : accessControlContext.getRoles();

        for (Accessor accessor : accessControl.accessors()) {
            if (accessor.access().allows(accessType)
                && (accessor.type() == Accessor.Type.Anyone
                    || (accessor.type() == Accessor.Type.Role && roles != null && roles.contains(accessor.typeValue())))) {
                return true;
            }
        }

        return false;
    }


    private AccessControl getAccessControlAnnotation() {
        for (Class daoInterface : getClass().getInterfaces()) {
            // noinspection unchecked
            AccessControl accessControl = (AccessControl) daoInterface.getAnnotation(AccessControl.class);

            if (accessControl != null) {
                return accessControl;
            }
        }

        return null;
    }
}
