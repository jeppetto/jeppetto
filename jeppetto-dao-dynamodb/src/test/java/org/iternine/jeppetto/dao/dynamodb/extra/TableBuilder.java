/*
 * Copyright (c) 2011-2015 Jeppetto and Jonathan Thompson
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


package org.iternine.jeppetto.dao.dynamodb.extra;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import java.util.ArrayList;


public class TableBuilder {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String tableName;
    private ArrayList<KeySchemaElement> keySchema;
    private ArrayList<AttributeDefinition> attributeDefinitions;
    private ArrayList<LocalSecondaryIndex> localSecondaryIndexes;
    private ArrayList<GlobalSecondaryIndex> globalSecondaryIndexes;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public TableBuilder(String tableName) {
        this.tableName = tableName;
        this.keySchema = new ArrayList<KeySchemaElement>();
        this.attributeDefinitions = new ArrayList<AttributeDefinition>();
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public TableBuilder withKey(String hashKeyName) {
        return withKey(hashKeyName, ScalarAttributeType.S);
    }


    public TableBuilder withKey(String hashKeyName, ScalarAttributeType hashKeyType) {
        return withKey(hashKeyName, hashKeyType, null, null);
    }


    public TableBuilder withKey(String hashKeyName, String rangeKeyName) {
        return withKey(hashKeyName, ScalarAttributeType.S, rangeKeyName, ScalarAttributeType.S);
    }


    public TableBuilder withKey(String hashKeyName, ScalarAttributeType hashKeyType,
                                String rangeKeyName, ScalarAttributeType rangeKeyType) {
        keySchema.add(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(hashKeyName));
        attributeDefinitions.add(new AttributeDefinition(hashKeyName, hashKeyType));

        if (rangeKeyName != null) {
            keySchema.add(new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName(rangeKeyName));
            attributeDefinitions.add(new AttributeDefinition(rangeKeyName, rangeKeyType));
        }

        return this;
    }


    public TableBuilder withLsi(String indexKey) {
        return withLsi(indexKey, ScalarAttributeType.S);
    }
    

    public TableBuilder withLsi(String indexKey, ScalarAttributeType indexKeyType) {
        if (localSecondaryIndexes == null) {
            localSecondaryIndexes = new ArrayList<LocalSecondaryIndex>();
        }

        attributeDefinitions.add(new AttributeDefinition(indexKey, indexKeyType));

        localSecondaryIndexes.add(new LocalSecondaryIndex().withIndexName(indexKey + "-index")
                                                           .withKeySchema(new KeySchemaElement(keySchema.get(0).getAttributeName(), KeyType.HASH),
                                                                          new KeySchemaElement(indexKey, KeyType.RANGE))
                                                           .withProjection(new Projection().withProjectionType(ProjectionType.ALL)));

        return this;
    }


    public TableBuilder withGsi(String gsiHashKeyName) {
        return withGsi(gsiHashKeyName, ScalarAttributeType.S);
    }


    public TableBuilder withGsi(String gsiHashKeyName, ScalarAttributeType gsiHashKeyType) {
        return withGsi(gsiHashKeyName, gsiHashKeyType, null, null);
    }


    public TableBuilder withGsi(String gsiHashKeyName, String gsiRangeKeyName) {
        return withGsi(gsiHashKeyName, ScalarAttributeType.S, gsiRangeKeyName, ScalarAttributeType.S);
    }


    public TableBuilder withGsi(String gsiHashKeyName, ScalarAttributeType gsiHashKeyType,
                                String gsiRangeKeyName, ScalarAttributeType gsiRangeKeyType) {
        if (globalSecondaryIndexes == null) {
            globalSecondaryIndexes = new ArrayList<GlobalSecondaryIndex>();
        }

        ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        String indexName;

        attributeDefinitions.add(new AttributeDefinition(gsiHashKeyName, gsiHashKeyType));
        keySchema.add(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(gsiHashKeyName));

        if (gsiRangeKeyName != null) {
            attributeDefinitions.add(new AttributeDefinition(gsiRangeKeyName, gsiRangeKeyType));
            keySchema.add(new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName(gsiRangeKeyName));

            indexName = gsiHashKeyName + "-" + gsiRangeKeyName;
        } else {
            indexName = gsiHashKeyName;
        }

        globalSecondaryIndexes.add(new GlobalSecondaryIndex().withIndexName(indexName)
                                                             .withProvisionedThroughput(new ProvisionedThroughput(64L, 64L))
                                                             .withKeySchema(keySchema)
                                                             .withProjection(new Projection().withProjectionType(ProjectionType.ALL)));
        
        return this;
    }
    
    
    public void build(AmazonDynamoDB amazonDynamoDB) {
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                                                                        .withKeySchema(keySchema)
                                                                        .withAttributeDefinitions(attributeDefinitions)
                                                                        .withProvisionedThroughput(new ProvisionedThroughput(64L, 64L))
                                                                        .withLocalSecondaryIndexes(localSecondaryIndexes)
                                                                        .withGlobalSecondaryIndexes(globalSecondaryIndexes);

        amazonDynamoDB.createTable(createTableRequest);
    }
}
