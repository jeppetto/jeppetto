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

package org.iternine.jeppetto.dao.test.examples.gamescore;


import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;


/**
 */
public abstract class GameScoreTest {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    protected static final Date oneDayAgo = new Date((new Date()).getTime() - (1*24*60*60*1000));
    protected static final Date sevenDaysAgo = new Date((new Date()).getTime() - (7*24*60*60*1000));
    protected static final Date fourteenDaysAgo = new Date((new Date()).getTime() - (14*24*60*60*1000));
    protected static final Date twentyOneDaysAgo = new Date((new Date()).getTime() - (21*24*60*60*1000));


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract GameScoreDAO getGameScoreDAO();

    protected abstract UserProgressDAO getUserProgressDAO();

    protected abstract void reset();


    //-------------------------------------------------------------
    // Methods - Test Lifecycle
    //-------------------------------------------------------------

    @After
    public void after() {
        reset();
    }


    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Test
    public void findTopScores() {
        createData();

        Iterable<GameScore> gameScores = getGameScoreDAO().findByGameTitleOrderByTopScoreDesc("Galaxy Invaders");

        int lastTopScore = Integer.MAX_VALUE;
        int count = 0;
        for (GameScore gameScore : gameScores) {
            Assert.assertEquals("Galaxy Invaders", gameScore.getGameTitle());
            Assert.assertTrue(gameScore.getTopScore() <= lastTopScore);

            lastTopScore = gameScore.getTopScore();

            count++;
        }

        Assert.assertEquals(3, count);
    }


    @Test
    public void findZeroScores() {
        createData();

        getUserProgressDAO().save(new UserProgress("123", "Comet Quest", 0, oneDayAgo, 0, 1));
        getUserProgressDAO().save(new UserProgress("201", "Comet Quest", 0, oneDayAgo, 0, 1));
        getUserProgressDAO().save(new UserProgress("301", "Comet Quest", 0, oneDayAgo, 0, 1));
        getUserProgressDAO().save(new UserProgress("400", "Comet Quest", 1000, oneDayAgo, 1, 0));

        Iterable<GameScore> gameScores = getGameScoreDAO().findByGameTitleAndTopScore("Comet Quest", 0);

        int count = 0;
        for (GameScore gameScore : gameScores) {
            Assert.assertEquals("Comet Quest", gameScore.getGameTitle());
            Assert.assertEquals(0, gameScore.getTopScore());

            count++;
        }

        Assert.assertEquals(3, count);
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void createData() {
        getUserProgressDAO().save(new UserProgress("101", "Galaxy Invaders", 5842, oneDayAgo, 21, 72));
        getUserProgressDAO().save(new UserProgress("101", "Meteor Blasters", 1000, sevenDaysAgo, 12, 3));
        getUserProgressDAO().save(new UserProgress("101", "Starship X", 24, fourteenDaysAgo, 4, 9));

        getUserProgressDAO().save(new UserProgress("102", "Alien Adventure", 192, oneDayAgo, 32, 192));
        getUserProgressDAO().save(new UserProgress("102", "Galaxy Invaders", 0, sevenDaysAgo, 0, 5));

        getUserProgressDAO().save(new UserProgress("103", "Attack Ships", 3, oneDayAgo, 1, 8));
        getUserProgressDAO().save(new UserProgress("103", "Galaxy Invaders", 2317, sevenDaysAgo, 40, 3));
        getUserProgressDAO().save(new UserProgress("103", "Meteor Blasters", 723, fourteenDaysAgo, 22, 12));
        getUserProgressDAO().save(new UserProgress("103", "Starship X", 42, twentyOneDaysAgo, 4, 19));
    }
}
