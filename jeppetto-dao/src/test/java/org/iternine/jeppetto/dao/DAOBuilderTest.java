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


import org.iternine.jeppetto.dao.id.BaseNIdGenerator;

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


    @Test
    public void idTest() {
        double base10 = (Math.log(10) / Math.log(2));
        double base16 = (Math.log(16) / Math.log(2));
        double base36 = (Math.log(36) / Math.log(2));
        double base62 = (Math.log(62) / Math.log(2));

        System.out.println(" c\t|  base10\t|  base16\t|  base36\t|  base64");
        System.out.println("---------------------------------------------------");
        for (int c = 1; c <= 64; c++) {
            System.out.println(String.format("%2d\t|  %6.2f\t|  %6.2f\t|  %6.2f\t|  %6.2f", c, c * base10, c * base16, c * base36, c * base62));
        }

//        BaseNIdGenerator baseNIdGenerator = new BaseNIdGenerator(128, BaseNIdGenerator.BASE36_CHARACTERS);
//
//        for (int i = 0; i < 100; i++) {
//            String id = baseNIdGenerator.generateId();
//
//            System.out.println(id);
//        }
    }
}
