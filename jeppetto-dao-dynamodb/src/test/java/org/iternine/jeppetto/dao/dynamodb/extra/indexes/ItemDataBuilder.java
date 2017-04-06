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


public class ItemDataBuilder {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    public static final String VARIABLE = "VARIABLE";


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private int itemCount;
    private String hashKey;
    private String rangeKey;
    private String lsiField;
    private String gsiHashKey;
    private String gsiRangeKey;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public ItemDataBuilder(int itemCount) {
        this.itemCount = itemCount;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public ItemDataBuilder withKeyData(String hashKey) {
        return withKeyData(hashKey, null);
    }


    public ItemDataBuilder withKeyData(String hashKey, String rangeKey) {
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;

        return this;
    }


    public ItemDataBuilder withLsiData(String lsiField) {
        this.lsiField = lsiField;

        return this;
    }


    public ItemDataBuilder withGsiData(String gsiHashKey) {
        return withGsiData(gsiHashKey, null);
    }


    public ItemDataBuilder withGsiData(String gsiHashKey, String gsiRangeKey) {
        this.gsiHashKey = gsiHashKey;
        this.gsiRangeKey = gsiRangeKey;

        return this;
    }


    @SuppressWarnings("StringEquality")
    public void build(ItemDAO itemDAO) {
        for (int i = 0; i < itemCount; i++) {
            Item item = new Item();

            item.setHashKey(hashKey == VARIABLE ? "H_" + i : hashKey);
            item.setRangeKey(rangeKey == VARIABLE ? "R_" + i : rangeKey);
            item.setLsiField(lsiField == VARIABLE ? "L_" + i : lsiField);
            item.setGsiHashKey(gsiHashKey == VARIABLE ? "GH_" + i : gsiHashKey);
            item.setGsiRangeKey(gsiRangeKey == VARIABLE ? "GR_" + i : gsiRangeKey);

            itemDAO.save(item);
        }
    }
}
