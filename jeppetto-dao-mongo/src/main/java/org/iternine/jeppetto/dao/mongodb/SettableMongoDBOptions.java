/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.mongodb;


import com.mongodb.MongoOptions;


public class SettableMongoDBOptions extends MongoOptions {

    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public int getConnectionsPerHost() {
        return connectionsPerHost;
    }


    public void setConnectionsPerHost(int connectionsPerHost) {
        this.connectionsPerHost = connectionsPerHost;
    }


    public int getThreadsAllowedToBlockForConnectionMultiplier() {
        return threadsAllowedToBlockForConnectionMultiplier;
    }


    public void setThreadsAllowedToBlockForConnectionMultiplier(int threadsAllowedToBlockForConnectionMultiplier) {
        this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
    }


    public int getConnectTimeout() {
        return connectTimeout;
    }


    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }


    public int getSocketTimeout() {
        return socketTimeout;
    }


    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }


    public boolean isAutoConnectRetry() {
        return autoConnectRetry;
    }


    public void setAutoConnectRetry(boolean autoConnectRetry) {
        this.autoConnectRetry = autoConnectRetry;
    }
}
