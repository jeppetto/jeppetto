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

package org.iternine.jeppetto.testsupport.db;


import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import org.dbunit.database.IDatabaseConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


public abstract class MongoDatabase extends Database {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Logger logger = LoggerFactory.getLogger(MongoDatabase.class);


    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private static File mongod;


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    public static MongoDatabase forPlatform(int port) {
        String platform = System.getProperty("os.name", "unknown").toLowerCase();

        if ("linux".equals(platform)) {
            return new LinuxOrMacMongoDatabase(platform, port).initDatabase();
        } else if ("mac os x".equals(platform)) {
            return new LinuxOrMacMongoDatabase("mac", port).initDatabase();
        } else {
            throw new RuntimeException("Unknown platform for mongodb test context: " + platform);
        }
    }


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String mongoDbName;
    private Process mongoProcess;
    private final int mongoDbPort;


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    public MongoDatabase(int port) {
        super(null);

        this.mongoDbPort = port;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public void setMongoDbName(String mongoDbName) {
        this.mongoDbName = mongoDbName;
    }


    //-------------------------------------------------------------
    // Implementation - Database
    //-------------------------------------------------------------

    @Override
    public void close() {
        if (mongoDbName == null) {
            return;
        }
        
        try {
            Mongo mongo = new Mongo("127.0.0.1", mongoDbPort);
            DB db = mongo.getDB(mongoDbName);

            db.resetError();
            db.dropDatabase();

            DBObject err = db.getLastError();
            if (err != null && err.get("err") != null) {
                logger.error("Could not drop database {}: {}", mongoDbName, err);
            }

            mongo.dropDatabase(mongoDbName);

            if (mongo.getDatabaseNames().contains(mongoDbName)) {
                logger.error("Database {} will not go away!", mongoDbName);
            }
        } catch (UnknownHostException e) {
            // weird
        } catch (MongoException e) {
            logger.warn("Could not drop database {}: {}", mongoDbName, e.getMessage());
        }
    }


    @Override
    protected void onNewIDatabaseConnection(IDatabaseConnection connection) {
        throw new UnsupportedOperationException("MongoDatabase does not support new IDatabaseConnections.");
    }


    //-------------------------------------------------------------
    // Methods - Protected - Abstract
    //-------------------------------------------------------------

    protected abstract String getPlatform();


    protected abstract void makeExecutable(File file);


    protected abstract ProcessBuilder createMongoProcess(File mongod, File dbpath, int port)
            throws IOException;


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected MongoDatabase initDatabase() {
        if (alreadyRunning(mongoDbPort)) {
            logger.debug("Mongo already running, using existing server.");

            return this;
        } else {
            return downloadExtractAndStartMongo();
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private int getMongoDbWebPort() {
        return mongoDbPort + 1000;
    }


    private boolean alreadyRunning(int port) {
        try {
            Socket socket = new Socket("localhost", port);

            socket.close();

            return true;
        } catch (IOException e) {
            return false;
        }
    }


    private MongoDatabase downloadExtractAndStartMongo() {
        try {
            File dbpath = createDataDir();

            ProcessBuilder processBuilder = createMongoProcess(findMongod(), dbpath, mongoDbPort).redirectErrorStream(true);
            mongoProcess = processBuilder.start();

            // this thread will die when the process does
            Thread readerDaemon = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final BufferedReader reader = new BufferedReader(new InputStreamReader(mongoProcess.getInputStream()));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            logger.debug(line);
                        }
                    } catch (IOException e) {
                        // bury
                    }

                    logger.debug("Mongod out consumer daemon exit.");
                }
            });
            readerDaemon.setDaemon(true);
            readerDaemon.start();

            if (!waitForMongo(getMongoDbWebPort()) ) {
                try {
                    throw new RuntimeException("Mongod process aborted with code " + mongoProcess.exitValue());
                } catch (IllegalThreadStateException e) {
                    throw new RuntimeException("Mongod process still running but it doesn't seem to be running normally.");
                }
            }

            logger.debug("Mongod seems to be running: http://localhost:{}", getMongoDbWebPort());

        } catch (Exception e) {
            this.close();
            throw new RuntimeException(e);
        }

        return this;
    }


    private File findMongod()
            throws IOException {
        if (mongod != null) {
            return mongod;
        }

        File mongoArchive = findMongoArchive();

        if (!mongoArchive.exists()) {
            throw new RuntimeException("Mongo archive not found at: " + mongoArchive.getAbsolutePath());
        }

        logger.debug(String.format("Extracting mongod from %s%n", mongoArchive.getAbsolutePath()));

        ZipFile zip = new ZipFile(mongoArchive);

        // TODO : this won't work on windows
        ZipEntry mongodEntry = zip.getEntry("bin/mongod");
        InputStream input = zip.getInputStream(mongodEntry);

        mongod = new File(System.getProperty("java.io.tmpdir", "/tmp"), "mongod-bin/mongod");
        if (!mongod.getParentFile().mkdirs()) {
            logger.warn("Could not create parent dir for mongod, either a permission issue or dir was left behind by previous process.");
        }

        copy(input, mongod);

        zip.close();

        logger.debug(String.format("Copied mongod binary to %s%n", mongod.getAbsolutePath()));

        return mongod;
    }


    private File findMongoArchive() {
        String mavenLocalRepo = System.getProperty("maven.repo.local");
        String actualRepo = (mavenLocalRepo == null
                             || mavenLocalRepo.isEmpty()
                             || mavenLocalRepo.contains("$")) ? System.getProperty("user.home") + "/.m2/repository"
                                                              : mavenLocalRepo;
        
        return new File(String.format("%3$s/org/mongodb/mongod-binary/%1$s/mongod-binary-%1$s-%2$s.zip",
                                      getMongoVersion(),
                                      getPlatform(),
                                      actualRepo));
    }


    private File createDataDir() {
        File tmpDir = new File(System.getProperty("java.io.tmpdir", "/tmp"));
        File datadir = new File(tmpDir, "mongodb-data");

        if (!datadir.mkdirs()) {
            logger.warn("Could not create data dir {}. Either a permissions issue, or the data dir was left behind by a previous process.",
                        datadir.getAbsolutePath());
        }

        return datadir;
    }


    private String getMongoVersion() {
        Properties mongoProps = new Properties();

        try {
            mongoProps.load(getClass().getClassLoader().getResourceAsStream("mongo.properties"));

            return mongoProps.getProperty("mongo.version");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private boolean waitForMongo(int port)
            throws InterruptedException {
        int secondsToWait = 7;

        for (int i = 0; i < secondsToWait + 1; i++) {
            if (alreadyRunning(port)) {
                return true;
            } else if (i < secondsToWait) {
                Thread.sleep(1000L);
            }
        }

        return false;
    }


    private void copy(InputStream input, File output)
            throws IOException {
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(output));

        byte[] buffer = new byte[10240];
        int len;
        while ((len = input.read(buffer)) >= 0) {
            out.write(buffer, 0, len);
        }

        input.close();
        out.close();

        makeExecutable(output);
    }


    //-------------------------------------------------------------
    // Inner Classes
    //-------------------------------------------------------------

    private static class LinuxOrMacMongoDatabase
            extends MongoDatabase {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private String platform;


        //-------------------------------------------------------------
        // Constructor
        //-------------------------------------------------------------

        public LinuxOrMacMongoDatabase(String platform, int port) {
            super(port);

            this.platform = platform;
        }


        //-------------------------------------------------------------
        // Implementation - MongoDatabase
        //-------------------------------------------------------------

        @Override
        protected String getPlatform() {
            return platform;
        }


        @Override
        protected void makeExecutable(File file) {
            ProcessBuilder pb = new ProcessBuilder("chmod", "+x", file.getAbsolutePath());

            try {
                Process p = pb.start();
                int exit = p.waitFor();

                if (exit != 0) {
                    throw new RuntimeException("chmod of " + file.getAbsolutePath() + " returned " + exit);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected ProcessBuilder createMongoProcess(File mongod, File dbpath, int port)
                throws IOException {
            return new ProcessBuilder(mongod.getAbsolutePath(),
                                      "--dbpath", dbpath.getAbsolutePath(),
                                      "--quiet",
                                      "--bind_ip", "localhost",
                                      "--smallfiles",
                                      "--noprealloc",
                                      "--port", Integer.toString(port));
        }
    }
}
