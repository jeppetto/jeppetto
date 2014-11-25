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

package org.iternine.jeppetto.dao.dynamodb;


import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.ConditionType;
import org.iternine.jeppetto.dao.FailedBatchException;
import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.OptimisticLockException;
import org.iternine.jeppetto.dao.Pair;
import org.iternine.jeppetto.dao.Projection;
import org.iternine.jeppetto.dao.ProjectionType;
import org.iternine.jeppetto.dao.QueryModel;
import org.iternine.jeppetto.dao.QueryModelDAO;
import org.iternine.jeppetto.dao.Sort;
import org.iternine.jeppetto.dao.SortDirection;
import org.iternine.jeppetto.dao.TooManyItemsException;
import org.iternine.jeppetto.dao.dynamodb.iterable.BatchGetIterable;
import org.iternine.jeppetto.dao.dynamodb.iterable.QueryIterable;
import org.iternine.jeppetto.dao.dynamodb.iterable.ScanIterable;
import org.iternine.jeppetto.dao.id.IdGenerator;
import org.iternine.jeppetto.dao.updateobject.UpdateObject;
import org.iternine.jeppetto.enhance.Enhancer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of the QueryModelDAO interface that works atop DynamoDB.
 *
 * @param <T> Persistent class
 * @param <ID> ID type of the persistent class.
 */
public class DynamoDBQueryModelDAO<T, ID>
        implements QueryModelDAO<T, ID> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBQueryModelDAO.class);


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final Class<T> entityClass;
    private final AmazonDynamoDB dynamoDB;
    private final String tableName;
    private final IdGenerator<ID> idGenerator;
    private final boolean consistentRead;

    private final String hashKeyField;
    private final String rangeKeyField;
    private final Map<String, String> localIndexes;
//    private Map<Pair<String, String>, String> globalIndexes;
    private final Enhancer<T> persistableEnhancer;
    private final Enhancer<? extends T> updateObjectEnhancer;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected DynamoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties) {
        this(entityClass, daoProperties, null);
    }


    @SuppressWarnings({ "unchecked", "UnusedParameters" })
    protected DynamoDBQueryModelDAO(Class<T> entityClass, Map<String, Object> daoProperties,
                                    AccessControlContextProvider accessControlContextProvider) {
        this.entityClass = entityClass;
        this.dynamoDB = (AmazonDynamoDB) daoProperties.get("db");
        this.tableName = daoProperties.containsKey("tableName") ? (String) daoProperties.get("tableName") : entityClass.getSimpleName();
        this.idGenerator = (IdGenerator<ID>) daoProperties.get("idGenerator");
        this.consistentRead = Boolean.parseBoolean((String) daoProperties.get("consistentRead"));   // null okay

        TableDescription tableDescription = dynamoDB.describeTable(tableName).getTable();

        Pair<String, String> primaryKeyAttributeNames = getKeyAttributeNames(tableDescription.getKeySchema());
        this.hashKeyField = primaryKeyAttributeNames.getFirst();
        this.rangeKeyField = primaryKeyAttributeNames.getSecond();

        List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription.getLocalSecondaryIndexes();
        if (localSecondaryIndexes != null) {
            this.localIndexes = new HashMap<String, String>(localSecondaryIndexes.size() + 1);

            for (LocalSecondaryIndexDescription description : localSecondaryIndexes) {
                localIndexes.put(getKeyAttributeNames(description.getKeySchema()).getSecond(), description.getIndexName());
            }

            // We include the primary range key as an local index (w/o an index name) to make findUsingQueryModel() code below simpler
            localIndexes.put(rangeKeyField, null);
        } else {
            this.localIndexes = Collections.singletonMap(rangeKeyField, null);
        }

        // TODO: Handle GSIs
//        List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = tableDescription.getGlobalSecondaryIndexes();
//        if (!globalSecondaryIndexes.isEmpty()) {
//            globalIndexes = new HashMap<Pair<String, String>, String>(globalSecondaryIndexes.size());
//
//            for (GlobalSecondaryIndexDescription description : globalSecondaryIndexes) {
//                globalIndexes.put(getKeyAttributeNames(description.getKeySchema()), description.getIndexName());
//            }
//        }

        this.persistableEnhancer = EnhancerHelper.getPersistableEnhancer(entityClass);

        String updateObjectClassName = (String) daoProperties.get("updateObject");
        if (updateObjectClassName == null) {
            this.updateObjectEnhancer = EnhancerHelper.getUpdateObjectEnhancer(entityClass);
        } else {
            try {
                Class updateObjectClass = Class.forName(updateObjectClassName);

                if (!entityClass.isAssignableFrom(updateObjectClass)) {
                    throw new JeppettoException(String.format("Invalid UpdateObject type. %s does not subclass entity type %s",
                                                              updateObjectClassName, entityClass.getName()));
                }

                this.updateObjectEnhancer = (Enhancer<? extends T>) EnhancerHelper.getUpdateObjectEnhancer(updateObjectClass);
            } catch (ClassNotFoundException e) {
                throw new JeppettoException(e);
            }
        }
    }


    //-------------------------------------------------------------
    // Implementation - GenericDAO
    //-------------------------------------------------------------

    @Override
    public T findById(ID id)
            throws NoSuchItemException, JeppettoException {
        GetItemResult result;

        try {
            result = dynamoDB.getItem(new GetItemRequest(tableName, getKeyFrom(id)));
        } catch (AmazonClientException e) {
            throw new JeppettoException(e);
        }

        if (result.getItem() == null) {
            throw new NoSuchItemException(entityClass.getSimpleName(), id.toString());
        }

        T t = ConversionUtil.getObjectFromItem(result.getItem(), entityClass);

        ((DynamoDBPersistable) t).__markPersisted(dynamoDB.toString());

        return t;
    }


    @Override
    public Iterable<T> findByIds(ID... ids)
            throws JeppettoException {
        Collection<Map<String, AttributeValue>> keys = new ArrayList<Map<String, AttributeValue>>();

        for (ID id : ids) {
            keys.add(getKeyFrom(id));
        }

        Map<String, KeysAndAttributes> requestItems = Collections.singletonMap(tableName, new KeysAndAttributes().withKeys(keys));
        BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest().withRequestItems(requestItems);

        return new BatchGetIterable<T>(dynamoDB, persistableEnhancer, batchGetItemRequest, tableName);
    }


    @Override
    public Iterable<T> findAll()
            throws JeppettoException {
        return findUsingQueryModel(new QueryModel());
    }


    @Override
    public void save(T entity)
            throws OptimisticLockException, JeppettoException {
        DynamoDBPersistable dynamoDBPersistable = (DynamoDBPersistable) persistableEnhancer.enhance(entity);

        if (!dynamoDBPersistable.__isPersisted(dynamoDB.toString())) {
            saveItem(dynamoDBPersistable);
        } else {
            updateItem(getKeyFrom(dynamoDBPersistable), new UpdateExpressionBuilder(dynamoDBPersistable), null);
        }

        dynamoDBPersistable.__markPersisted(dynamoDB.toString());
    }


    @Override
    public void delete(T entity)
            throws JeppettoException {
        if (entity == null) {
            throw new JeppettoException("entity is null; nothing to delete.");
        }

        deleteItem(getKeyFrom((DynamoDBPersistable) persistableEnhancer.enhance(entity)));
    }


    @Override
    public void deleteById(ID id)
            throws JeppettoException {
        if (id == null) {
            throw new JeppettoException("id is null; unable to delete entity.");
        }

        deleteItem(getKeyFrom(id));
    }


    @Override
    public void deleteByIds(ID... ids)
            throws FailedBatchException, JeppettoException {
        Map<ID, Exception> failedDeletes = new LinkedHashMap<ID, Exception>();

        for (ID id : ids) {
            try {
                deleteItem(getKeyFrom(id));
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
        //noinspection unchecked
        return (U) updateObjectEnhancer.newInstance();
    }


    @Override
    public <U extends T> void updateByIds(U updateObject, ID... ids)
            throws FailedBatchException, JeppettoException {
        UpdateExpressionBuilder updateExpressionBuilder = new UpdateExpressionBuilder((UpdateObject) updateObject);
        Map<ID, Exception> failedUpdates = new LinkedHashMap<ID, Exception>();

        for (ID id : ids) {
            try {
                updateItem(getKeyFrom(id), updateExpressionBuilder, null);
            } catch (Exception e) {
                failedUpdates.put(id, e);
            }
        }

        if (failedUpdates.size() > 0) {
            throw new FailedBatchException("Unable to update all items", failedUpdates);
        }
    }


    @Override
    public void flush()
            throws JeppettoException {
        // Flush not required.
    }


    //-------------------------------------------------------------
    // Implementation - QueryModelDAO
    //-------------------------------------------------------------

    @Override
    public T findUniqueUsingQueryModel(QueryModel queryModel)
            throws NoSuchItemException, TooManyItemsException, JeppettoException {
        queryModel.setMaxResults(2);

        Iterator<T> results = findUsingQueryModel(queryModel).iterator();

        if (!results.hasNext()) {
            throw new NoSuchItemException();
        }

        T result = results.next();

        if (results.hasNext()) {
            throw new TooManyItemsException();
        }

        return result;
    }


    @Override
    public Iterable<T> findUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        ConditionExpressionBuilder conditionExpressionBuilder = new ConditionExpressionBuilder(queryModel, hashKeyField, localIndexes);

        if (conditionExpressionBuilder.hasHashKeyCondition()) {
            return queryItems(queryModel, conditionExpressionBuilder);
        } else {
            logger.warn("Condition does not have a hash key specified -- using 'scan' to search.");

            conditionExpressionBuilder.convertRangeKeyConditionToExpression();

            return scanItems(queryModel, conditionExpressionBuilder);
        }
    }


    @Override
    public Object projectUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        // TODO: handle count case.  not sure about other projections...

        throw new UnsupportedOperationException("Projections on DynamoDB are not currently supported.");
    }


    @Override
    public void deleteUsingQueryModel(QueryModel queryModel)
            throws JeppettoException {
        Iterable<T> matches = findUsingQueryModel(queryModel);

        // Would ideally catch individual exceptions and throw a FailedBatchException.  Unfortunately, we don't have an easy
        // way to convert the key from a match to an ID object, which is what the exception's Map contains.  Maybe DynamoDB will
        // add support for a query-based delete.
        for (T match : matches) {
            delete(match);
        }
    }


    @Override
    public <U extends T> void updateUsingQueryModel(U updateObject, QueryModel queryModel)
            throws JeppettoException {
        UpdateExpressionBuilder updateExpressionBuilder = new UpdateExpressionBuilder((UpdateObject) updateObject);
        // For referencing an object, we can only identify an item by its actual range key, not one of the index fields.  For the
        // third parameter, pass in a singleton map that only contains the range field.
        ConditionExpressionBuilder conditionExpressionBuilder
                = new ConditionExpressionBuilder(queryModel, hashKeyField, Collections.singletonMap(rangeKeyField, (String) null));

        Map<String, AttributeValue> key;

        try {
            key = conditionExpressionBuilder.getKey();
        } catch (NullPointerException e) {
            throw new JeppettoException("DynamoDB only supports updates where the condition uniquely identifies the item by its key.", e);
        }

        updateItem(key, updateExpressionBuilder, conditionExpressionBuilder);
    }


    @Override
    public Condition buildCondition(String conditionField, ConditionType conditionType, Iterator argsIterator) {
        return new Condition(conditionField, DynamoDBOperator.valueOf(conditionType.name()).buildConstraint(argsIterator));
    }


    @Override
    public Projection buildProjection(String projectionField, ProjectionType projectionType, Iterator argsIterator) {
        throw new UnsupportedOperationException("Projections on DynamoDB are not currently supported.");
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void saveItem(DynamoDBPersistable dynamoDBPersistable) {
        generateIdIfNeeded(dynamoDBPersistable);

        try {
            dynamoDB.putItem(new PutItemRequest(tableName, ConversionUtil.getItemFromObject(dynamoDBPersistable)));
        } catch (Exception e) {
            throw new JeppettoException(e);
        }
    }


    private void updateItem(Map<String, AttributeValue> key, UpdateExpressionBuilder updateExpressionBuilder,
                            ConditionExpressionBuilder conditionExpressionBuilder) {
        try {
            UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName)
                                                                         .withKey(key)
                                                                         .withUpdateExpression(updateExpressionBuilder.getExpression());

            Map<String, AttributeValue> attributeValues;

            if (conditionExpressionBuilder == null) {
                attributeValues = updateExpressionBuilder.getAttributeValues();
            } else {
                attributeValues = new LinkedHashMap<String, AttributeValue>();
                attributeValues.putAll(updateExpressionBuilder.getAttributeValues());
                attributeValues.putAll(conditionExpressionBuilder.getAttributeValues());

                updateItemRequest.withConditionExpression(conditionExpressionBuilder.getExpression());
            }

            if (!attributeValues.isEmpty()) {
                updateItemRequest.withExpressionAttributeValues(attributeValues);
            }

            dynamoDB.updateItem(updateItemRequest);
        } catch (Exception e) {
            throw new JeppettoException(e);
        }
    }


    private void deleteItem(Map<String, AttributeValue> key) {
        try {
            dynamoDB.deleteItem(new DeleteItemRequest(tableName, key));
        } catch (Exception e) {
            throw new JeppettoException(e);
        }
    }


    private Iterable<T> queryItems(QueryModel queryModel, ConditionExpressionBuilder conditionExpressionBuilder) {
        QueryRequest queryRequest = new QueryRequest(tableName);

        queryRequest.setKeyConditions(conditionExpressionBuilder.getKeyConditions());
        queryRequest.setIndexName(localIndexes.get(conditionExpressionBuilder.getRangeKey()));
        queryRequest.setConsistentRead(consistentRead);

        if (queryModel.getFirstResult() > 0) {
            logger.warn("DynamoDB does not support skipping results.  Call setPosition() on DynamoDBIterable instead.");
        }

        if (queryModel.getMaxResults() > 0) {
            queryRequest.setLimit(queryModel.getMaxResults());
        }

        if (queryModel.getSorts() != null) {
            for (Sort sort : queryModel.getSorts()) {
                if (!sort.getField().equals(conditionExpressionBuilder.getRangeKey())) {
                    logger.warn("Unable to sort on other than the acting range key. Ignoring sort on " + sort.getField());
                }

                queryRequest.setScanIndexForward(sort.getSortDirection() == SortDirection.Ascending);
            }
        }

        if (conditionExpressionBuilder.hasExpression()) {
            queryRequest.setFilterExpression(conditionExpressionBuilder.getExpression());
            queryRequest.setExpressionAttributeValues(conditionExpressionBuilder.getAttributeValues());
        }

        // TODO: if partial object, add attributes to fetch
//            queryRequest.setAttributesToGet();

        return new QueryIterable<T>(dynamoDB, persistableEnhancer, queryRequest, hashKeyField);
    }


    private Iterable<T> scanItems(QueryModel queryModel, ConditionExpressionBuilder conditionExpressionBuilder) {
        ScanRequest scanRequest = new ScanRequest(tableName);

        if (queryModel.getFirstResult() > 0) {
            logger.warn("DynamoDB does not support skipping results.  Call setPosition() on DynamoDBIterable instead.");
        }

        if (queryModel.getMaxResults() > 0) {
            scanRequest.setLimit(queryModel.getMaxResults());
        }

        if (queryModel.getSorts() != null) {
            logger.warn("Not able to sort when performing a 'scan' operation.  Ignoring... ");
        }

        if (conditionExpressionBuilder.hasExpression()) {
            scanRequest.setFilterExpression(conditionExpressionBuilder.getExpression());
            scanRequest.setExpressionAttributeValues(conditionExpressionBuilder.getAttributeValues());
        }

        // TODO: if partial object, add attributes to fetch
//            scanRequest.setAttributesToGet();

        return new ScanIterable<T>(dynamoDB, persistableEnhancer, scanRequest, hashKeyField);
    }


    private void generateIdIfNeeded(DynamoDBPersistable dynamoDBPersistable) {
        if (dynamoDBPersistable.__get(hashKeyField) != null
         /* && rangeKeyField != null && dynamoDBPersistable.__get(rangeKeyField) != null */) {
            return;
        }

        // TODO: handle case when part of the key is there (e.g. code generates range key, but wants to generate hash key)
        // Can't blindly use getKeyFrom since a single generated value may be for the range key...
        dynamoDBPersistable.__putAll(getKeyFrom(idGenerator.generateId()));
    }


    private AttributeValue getAttributeValue(Object value) {
        if (Number.class.isAssignableFrom(value.getClass())) {
            return new AttributeValue().withN(value.toString());
        } else {
            return new AttributeValue(value.toString());
        }
    }


    private Pair<String, String> getKeyAttributeNames(List<KeySchemaElement> keySchema) {
        Pair<String, String> keyAttributes = new Pair<String, String>();

        for (KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.getKeyType().equals(KeyType.HASH.name())) {
                keyAttributes.setFirst(keySchemaElement.getAttributeName());
            } else {
                keyAttributes.setSecond(keySchemaElement.getAttributeName());
            }
        }
        return keyAttributes;
    }


    private Map<String, AttributeValue> getKeyFrom(ID id) {
        Map<String, AttributeValue> key;

        if (Pair.class.isAssignableFrom(id.getClass())) {
            key = new HashMap<String, AttributeValue>(2);

            key.put(hashKeyField, getAttributeValue(((Pair) id).getFirst()));
            key.put(rangeKeyField, getAttributeValue(((Pair) id).getSecond()));
        } else {
            key = Collections.singletonMap(hashKeyField, getAttributeValue(id));
        }

        return key;
    }


    public Map<String, AttributeValue> getKeyFrom(DynamoDBPersistable dynamoDBPersistable) {
        Map<String, AttributeValue> key;

        if (rangeKeyField != null) {
            key = new HashMap<String, AttributeValue>(2);

            key.put(hashKeyField, ConversionUtil.toAttributeValue(dynamoDBPersistable.__get(hashKeyField)));
            key.put(rangeKeyField, ConversionUtil.toAttributeValue(dynamoDBPersistable.__get(rangeKeyField)));
        } else {
            key = Collections.singletonMap(hashKeyField, ConversionUtil.toAttributeValue(dynamoDBPersistable.__get(
                    hashKeyField)));
        }

        return key;
    }
}
