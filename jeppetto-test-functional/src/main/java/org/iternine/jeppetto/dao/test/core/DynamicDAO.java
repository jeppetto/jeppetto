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

package org.iternine.jeppetto.dao.test.core;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.test.SimpleObject;

import java.util.List;
import java.util.Set;


public interface DynamicDAO extends GenericDAO<SimpleObject, String> {

    SimpleObject findByIntValue(int intValue);


    SimpleObject findByAnotherIntValue(int intValue)
            throws NoSuchItemException;


    List<SimpleObject> findByIntValueAndAnotherIntValueGreaterThan(int intValue, int anotherIntValue);


    SimpleObject findByHavingRelatedObjectWithRelatedIntValue(int relatedIntValue);


    List<SimpleObject> findByOrderByIntValueDesc();


    Set<SimpleObject> findByIntValueGreaterThan(int intValue);


    SimpleObject findByLongValue(long longValue)
            throws NoSuchItemException;


    List<SimpleObject> findByIntValueWithin(List<Integer> someInts);


    List<SimpleObject> findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueDesc(int relatedIntValueMax);


    Iterable<SimpleObject> findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValue(int relatedIntValueMax);


    List<SimpleObject> findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimit(int relatedIntValueMax, int limit);


    List<SimpleObject> findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimitAndSkip(int relatedIntValueMax, int limit, int skipCount);


    void deleteByIntValueWithin(List<Integer> someInts);


    List<SimpleObject> findByRelatedObjectIsNotNull();


    SimpleObject findByRelatedObjectIsNull();


    List<SimpleObject> findByStringValueBeginsWith(String prefix);


    List<SimpleObject> findByHavingRelatedObjectWithRelatedStringValueBeginsWith(String prefix);
}
