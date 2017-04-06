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

package org.iternine.jeppetto.dao.test.examples.forum;


import java.util.Date;
import java.util.Set;


/**
 */
public class Thread {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String forumName;
    private String subject;
    private String message;
    private String lastPostedBy;
    private int views;
    private int replies;
    private int answered;
    private Set<String> tags;
    private Date lastPostedDate;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public Thread() {
    }


    public Thread(String forumName, String subject, String message, String lastPostedBy, int views, int replies,
                  int answered, Set<String> tags, Date lastPostedDate) {
        this.forumName = forumName;
        this.subject = subject;
        this.message = message;
        this.lastPostedBy = lastPostedBy;
        this.views = views;
        this.replies = replies;
        this.answered = answered;
        this.tags = tags;
        this.lastPostedDate = lastPostedDate;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getForumName() {
        return forumName;
    }


    public void setForumName(String forumName) {
        this.forumName = forumName;
    }


    public String getSubject() {
        return subject;
    }


    public void setSubject(String subject) {
        this.subject = subject;
    }


    public String getMessage() {
        return message;
    }


    public void setMessage(String message) {
        this.message = message;
    }


    public String getLastPostedBy() {
        return lastPostedBy;
    }


    public void setLastPostedBy(String lastPostedBy) {
        this.lastPostedBy = lastPostedBy;
    }


    public int getViews() {
        return views;
    }


    public void setViews(int views) {
        this.views = views;
    }


    public int getReplies() {
        return replies;
    }


    public void setReplies(int replies) {
        this.replies = replies;
    }


    public int getAnswered() {
        return answered;
    }


    public void setAnswered(int answered) {
        this.answered = answered;
    }


    public Set<String> getTags() {
        return tags;
    }


    public void setTags(Set<String> tags) {
        this.tags = tags;
    }


    public Date getLastPostedDate() {
        return lastPostedDate;
    }


    public void setLastPostedDate(Date lastPostedDate) {
        this.lastPostedDate = lastPostedDate;
    }
}
