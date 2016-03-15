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

package org.iternine.jeppetto.dao;


public enum ProjectionType {
    RowCount,           // field not required; int returned
    Count,              // field required; int returned
    CountDistinct,      // field required; int returned
    Maximum,            // field required; field type returned (e.g. int, double, Date)
    Minimum,            // field required; field type returned (e.g. int, double, Date)
    Average,            // field required; double returned
    Sum                 // field required; field type returned (e.g. int, double)
}
