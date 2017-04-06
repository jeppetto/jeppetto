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


import org.iternine.jeppetto.dao.dynamodb.extra.TableBuilder;

import org.junit.Test;

import static org.iternine.jeppetto.dao.dynamodb.extra.indexes.ItemDataBuilder.VARIABLE;


/**
 */
public class DynamoDBIndexTest extends BaseDynamoDBIndexTest {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final int PAGE_SIZE = 3;


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void testLsiBasedPagination() {
        new TableBuilder("Item").withKey("hashKey", "rangeKey").withLsi("lsiField").build(amazonDynamoDB);

        ItemDAO itemDAO = getItemDAO();
        String hashKeyValue = "abc";
        String lsiFieldValue = "def";
        int itemCount = 8;

        new ItemDataBuilder(itemCount).withKeyData(hashKeyValue, VARIABLE).withLsiData(lsiFieldValue).build(itemDAO);

        String queryPosition = null;
        int remaining = itemCount;

        do {
            queryPosition = getPage(itemDAO.findByHashKeyAndLsiField(hashKeyValue, lsiFieldValue), PAGE_SIZE, queryPosition, Math.min(PAGE_SIZE, remaining));
            remaining -= PAGE_SIZE;
        } while (queryPosition != null);
    }


    @Test
    public void testGsiBasedPagination() {
        new TableBuilder("Item").withKey("hashKey").withGsi("gsiHashKey", "gsiRangeKey").build(amazonDynamoDB);

        ItemDAO itemDAO = getItemDAO();
        String gsiHashKeyValue = "abc";
        int itemCount = 6;

        new ItemDataBuilder(itemCount).withKeyData(VARIABLE).withGsiData(gsiHashKeyValue, VARIABLE).build(itemDAO);

        String queryPosition = null;
        int remaining = itemCount;

        do {
            queryPosition = getPage(itemDAO.findByGsiHashKey(gsiHashKeyValue), PAGE_SIZE, queryPosition, Math.min(PAGE_SIZE, remaining));
            remaining -= PAGE_SIZE;
        } while (queryPosition != null);
    }


    @Test
    public void testGsiBasedPaginationWithPlusOneItems() {
        new TableBuilder("Item").withKey("hashKey").withGsi("gsiHashKey", "gsiRangeKey").build(amazonDynamoDB);

        ItemDAO itemDAO = getItemDAO();
        String gsiHashKeyValue = "abc";
        int itemCount = 7;

        new ItemDataBuilder(itemCount).withKeyData(VARIABLE).withGsiData(gsiHashKeyValue, VARIABLE).build(itemDAO);

        String queryPosition = null;
        int remaining = itemCount;

        do {
            queryPosition = getPage(itemDAO.findByGsiHashKey(gsiHashKeyValue), PAGE_SIZE, queryPosition, Math.min(PAGE_SIZE, remaining));
            remaining -= PAGE_SIZE;
        } while (queryPosition != null);
    }


    @Test
    public void testGsiBasedPaginationWithPageSizePlusTwoItems() {
        new TableBuilder("Item").withKey("hashKey").withGsi("gsiHashKey", "gsiRangeKey").build(amazonDynamoDB);

        ItemDAO itemDAO = getItemDAO();
        String gsiHashKeyValue = "abc";
        int itemCount = 8;

        new ItemDataBuilder(itemCount).withKeyData(VARIABLE).withGsiData(gsiHashKeyValue, VARIABLE).build(itemDAO);

        String queryPosition = null;
        int remaining = itemCount;

        do {
            queryPosition = getPage(itemDAO.findByGsiHashKey(gsiHashKeyValue), PAGE_SIZE, queryPosition, Math.min(PAGE_SIZE, remaining));
            remaining -= PAGE_SIZE;
        } while (queryPosition != null);
    }


    @Test
    public void testGsiBasedPaginationWithHashAndRangeKey() {
        new TableBuilder("Item").withKey("hashKey", "rangeKey").withGsi("gsiHashKey", "gsiRangeKey").build(amazonDynamoDB);

        ItemDAO itemDAO = getItemDAO();
        String gsiHashKeyValue = "abc";
        int itemCount = 8;

        new ItemDataBuilder(itemCount).withKeyData("abc", VARIABLE).withGsiData(gsiHashKeyValue, VARIABLE).build(itemDAO);

        String queryPosition = null;
        int remaining = itemCount;

        do {
            queryPosition = getPage(itemDAO.findByGsiHashKey(gsiHashKeyValue), PAGE_SIZE, queryPosition, Math.min(PAGE_SIZE, remaining));
            remaining -= PAGE_SIZE;
        } while (queryPosition != null);
    }


    @Test
    public void testGsiBasedPaginationWithHashOnly() {
        new TableBuilder("Item").withKey("hashKey").withGsi("gsiHashKey").build(amazonDynamoDB);

        ItemDAO itemDAO = getItemDAO();
        String gsiHashKeyValue = "GH_1";
        int itemCount = 8;

        new ItemDataBuilder(itemCount).withKeyData(VARIABLE).withGsiData(VARIABLE).build(itemDAO);

        String queryPosition = null;

        do {
            queryPosition = getPage(itemDAO.findByGsiHashKey(gsiHashKeyValue), PAGE_SIZE, queryPosition, 1);
        } while (queryPosition != null);
    }
}
