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

package org.iternine.jeppetto.dao.dynamodb.extra.indexes;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.dynamodb.iterable.DynamoDBIterable;


public interface ItemDAO extends GenericDAO<Item, String> {

    DynamoDBIterable<Item> findByHashKey(String hashKeyValue);

    Item findByHashKeyAndRangeKey(String hashKeyValue, String rangeKeyValue);

    DynamoDBIterable<Item> findByHashKeyAndLsiField(String hashKeyValue, String lsiFieldValue);

    // TODO: for projections DynamoDBIterable<Item> findByHashKeyAndLsiFieldKey2(String hashKeyValue, String lsiFieldValue2);

    DynamoDBIterable<Item> findByGsiHashKey(String gsiHashKeyValue);

    DynamoDBIterable<Item> findByGsiHashKeyAndGsiRangeKey(String gsiHashKeyValue, String gsiRangeKeyValue);

    // TODO: for projections DynamoDBIterable<Item> findByGsiHashKeyAndGsiRangeKey2(String gsiHashKeyValue, String gsiRangeKeyValue2);
}

