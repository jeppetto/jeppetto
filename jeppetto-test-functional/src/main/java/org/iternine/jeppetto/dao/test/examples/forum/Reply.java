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

package org.iternine.jeppetto.dao.test.examples.forum;


import java.util.Date;


/**
 */
public class Reply {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String id;
    private String message;
    private String postedBy;
    private Date replyDate;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public Reply() {
    }


    public Reply(String id, String message, String postedBy, Date replyDate) {
        this.id = id;
        this.message = message;
        this.postedBy = postedBy;
        this.replyDate = replyDate;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }


    public String getMessage() {
        return message;
    }


    public void setMessage(String message) {
        this.message = message;
    }


    public String getPostedBy() {
        return postedBy;
    }


    public void setPostedBy(String postedBy) {
        this.postedBy = postedBy;
    }


    public Date getReplyDate() {
        return replyDate;
    }


    public void setReplyDate(Date replyDate) {
        this.replyDate = replyDate;
    }
}
