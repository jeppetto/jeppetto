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

package org.iternine.jeppetto.dao;


public enum ConditionType {
    // NB: Make sure that values are in an order to ensure that name matching (for DSL-style criteria)
    // captures the longest matching operator.  For example, 'fooNotEqual' should generate 'foo' and
    // 'NotEqual', not 'fooNot' and 'Equal'.
    NotEqual,
    GreaterThanEqual,
    LessThanEqual,
    Equal,
    GreaterThan,
    LessThan,
    NotWithin,
    Within,
    Between,
    IsNull,
    IsNotNull,
    BeginsWith
}
