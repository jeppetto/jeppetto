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

package org.iternine.jeppetto.dao.test.updateobject;


import org.iternine.jeppetto.dao.ResultFromUpdate;
import org.iternine.jeppetto.dao.UpdateBehaviorDescriptor;
import org.iternine.jeppetto.dao.annotation.Transient;
import org.iternine.jeppetto.dao.test.SimpleObject;


/**
 */
public abstract class SimpleUpdateObject extends SimpleObject
        implements UpdateBehaviorDescriptor {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private ResultFromUpdate resultFromUpdate = ResultFromUpdate.ReturnNone;


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    public abstract void addToIntValue(int increment);

    public abstract void addToLongValue(long increment);

    public abstract void addToDoubleValue(double increment);


    //-------------------------------------------------------------
    // Implementation - UpdateBehaviorDescriptor
    //-------------------------------------------------------------

    @Override
    @Transient  // TODO: remove once DynamoDB implementation uses core UpdateObject
    public ResultFromUpdate getResultFromUpdate() {
        return resultFromUpdate;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public void setResultFromUpdate(ResultFromUpdate resultFromUpdate) {
        this.resultFromUpdate = resultFromUpdate;
    }
}
