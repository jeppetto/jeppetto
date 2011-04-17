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


import org.junit.Assert;
import org.junit.Test;

import java.util.List;


public class DAOBuilderTest {

    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Test
    public void checkBuilder() {
        SampleDAO sampleDAO = DAOBuilder.buildDAO(Sample.class,
                                                  SampleDAO.class,
                                                  PartialSampleDAOImplementation.class,
                                                  null);

        // TODO: reflect on sampleDAO for concrete implementations of methods found in SampleDAO
        // TODO: can check sampleDAO's real class type
        List<Sample> t = sampleDAO.getMany(0);

        Assert.assertTrue(t.size() > 1);
        sampleDAO.findByFieldOne(0);
        sampleDAO.findByFieldOneGreaterThan(1);
    }
}
