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

package org.jeppetto.dao;


import org.jeppetto.dao.annotation.Condition;
import org.jeppetto.dao.annotation.DataAccessMethod;

import java.util.List;


public interface SampleDAO extends GenericDAO<Sample, String> {

    Sample findByFieldOne(int fieldOneValue);


    List<Sample> findByFieldOneGreaterThan(int fieldOneValue);


    Iterable<Sample> findByFieldOneLessThanOrderByFieldOneDesc(int fieldOneValue);


    @DataAccessMethod(
            conditions = { @Condition(field = "fieldOne", type = ConditionType.GreaterThan) }
    )
    List<Sample> getMany(int fieldOneValue);
}
