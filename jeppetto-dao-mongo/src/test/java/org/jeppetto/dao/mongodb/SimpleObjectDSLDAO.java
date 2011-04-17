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

package org.jeppetto.dao.mongodb;


import org.jeppetto.dao.GenericDAO;
import org.jeppetto.dao.NoSuchItemException;

import java.util.List;
import java.util.Set;


public interface SimpleObjectDSLDAO extends GenericDAO<SimpleObject, String> {

    SimpleObject findByIntValue(int intValue);

    SimpleObject findByAnotherIntValue(int intValue)
        throws NoSuchItemException;

    List<SimpleObject> findByIntValueAndAnotherIntValueGreaterThan(int intValue, int anotherIntValue);

    SimpleObject findByIntValueHavingRelatedObjectWithRelatedIntValue(int intValue, int relatedIntValue);

    List<SimpleObject> findByIntValueGreaterThanOrderByIntValueDesc(int intValue);

    Set<SimpleObject> findByIntValueGreaterThan(int intValue);
}
