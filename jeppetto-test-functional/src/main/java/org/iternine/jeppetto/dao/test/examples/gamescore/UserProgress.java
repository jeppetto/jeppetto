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

package org.iternine.jeppetto.dao.test.examples.gamescore;


import java.util.Date;


/**
 */
public class UserProgress {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String userId;
    private String gameTitle;
    private int topScore;
    private Date topScoreDate;
    private int wins;
    private int losses;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public UserProgress() {
    }


    public UserProgress(String userId, String gameTitle, int topScore, Date topScoreDate, int wins, int losses) {
        this.userId = userId;
        this.gameTitle = gameTitle;
        this.topScore = topScore;
        this.topScoreDate = topScoreDate;
        this.wins = wins;
        this.losses = losses;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getUserId() {
        return userId;
    }


    public void setUserId(String userId) {
        this.userId = userId;
    }


    public String getGameTitle() {
        return gameTitle;
    }


    public void setGameTitle(String gameTitle) {
        this.gameTitle = gameTitle;
    }


    public int getTopScore() {
        return topScore;
    }


    public void setTopScore(int topScore) {
        this.topScore = topScore;
    }


    public Date getTopScoreDate() {
        return topScoreDate;
    }


    public void setTopScoreDate(Date topScoreDate) {
        this.topScoreDate = topScoreDate;
    }


    public int getWins() {
        return wins;
    }


    public void setWins(int wins) {
        this.wins = wins;
    }


    public int getLosses() {
        return losses;
    }


    public void setLosses(int losses) {
        this.losses = losses;
    }
}
