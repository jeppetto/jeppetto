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

package org.iternine.jeppetto.testsupport;


import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;


/**
 * This prvoides a convenient way to manage files for unit tests.
 * It allows for creating a per-test directory structure that can be used for temporary data as well as
 * load directory/file structures using zip archives.
 *
 * To use:
 * Create a FileSystemFixtureSupport. You can either specify a root directory or a name prefix to use
 * when creating the temporary directory. Your test can create new directories using <code>createDirectory()</code>
 * or load pre-built zip archives of directory structures into the temporary directory. In your test's
 * <code>@Before</code> method, you can cleanup the files/directories created during the test through the
 * <code>teardown()</code> method. You can also delete the temporary directory by calling the
 * <code>destroy()</code> method from an <code>@AfterClass</code> method. Once this is called, the fixture support
 * cannot be used.
 */
public class FileSystemFixtureSupport {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private File rootDir;
    private Set<File> fixtureFiles = new HashSet<File>();


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    /**
     * Create a FileSystemFixtureSupport that will create a new temporary directory using the specified
     * name prefix.
     * @param prefix the prefix used when naming the temporary directory this will manage
     */
    public FileSystemFixtureSupport(String prefix) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        rootDir = null;
        while(true) {
            rootDir = new File(tmpDir + File.separator + prefix + System.currentTimeMillis() + new Random().nextInt());
            if (! rootDir.exists()) {
                createDirectoryOrThrow(rootDir);
                return;
            }
        }
    }


    /**
     * Create a FileSystemFixtureSupport that will manage the specified File as its temporary root directory.
     * When destroy() is invoked, this directory will be deleted.
     * @param rootDir the temporary root directory. This will be created if it does not already exist.
     */
    public FileSystemFixtureSupport(File rootDir) {
        this.rootDir = rootDir;
        if (! rootDir.exists()) {
            createDirectoryOrThrow(rootDir);
        }
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    /**
     * Creates a new directory that is relative to the temporary testing root.
     * @param path the relative path of the new directory.                                                          
     * @return the new directory
     */
    public File createDirectory(String path) {
        verifyNotDestroyed();
        File newDirectory = new File(rootDir + File.separator + path);
        fixtureFiles.add(newDirectory);
        return newDirectory;
    }


    /**
     * Loads a directory structure from a zip archive. The archive will be extracted at the testing root dierectory.
     * @param resourcesToLoad the names of zip archives in the classpath that should be loaded.
     * @throws IOException if we cannot unarchive the files
     */
    public void loadFileFixtures(List<String> resourcesToLoad) throws IOException {
        verifyNotDestroyed();
        for (String resource: resourcesToLoad) {
            loadTestFixtureFromClasspath(resource);
        }
    }


    /**
     * Deletes any directories or files that have been created by this class.
     * @throws IOException if we are unable to delete the data
     */
    public void teardown()
            throws IOException {
        verifyNotDestroyed();
        List<String> filesThatWouldnotDelete = new ArrayList<String>(); 
        for (File fixtureFile: fixtureFiles) {

            if (fixtureFile.exists() && !deleteFile(fixtureFile)) {
                filesThatWouldnotDelete.add(fixtureFile.getAbsolutePath());
            }
        }

        fixtureFiles.clear();
        if (! filesThatWouldnotDelete.isEmpty()) {
            throw new IOException("Unable to delete the following files: " + filesThatWouldnotDelete); 
        }
    }


    /**
     * Deletes all of the fixture files as well as the temporary root directory. This should only be invoked when the
     * this class is no longer going to be used.
     * @throws IOException if deletion fails
     */
    public void destroy()
            throws IOException{
        teardown();
        if (! deleteFile(rootDir)) {
            throw new RuntimeException("Unable to delete contents of directory " + rootDir);
        }
        rootDir = null;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public File getTestingRoot() {
        verifyNotDestroyed();
        return rootDir;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void createDirectoryOrThrow(File dir) {
        if (! dir.mkdirs()) {
            throw new RuntimeException("Unable to create the specified directory " + dir);
        }
    }


    private void loadTestFixtureFromClasspath(String resourceName) throws IOException {
        ZipInputStream zipInputStream = null;
        FileOutputStream fileOutStream = null;
        try {
            zipInputStream = new ZipInputStream(ClassLoader.getSystemResourceAsStream(resourceName));
            ZipEntry nextEntry = null;
            while ((nextEntry = zipInputStream.getNextEntry()) != null) {
                String entryPath = nextEntry.getName();
                File outFile = new File(rootDir.getAbsolutePath() + File.separator + entryPath);
                if(! outFile.getParentFile().exists()) {
                    createDirectoryOrThrow(outFile.getParentFile());
                }
                fileOutStream = new FileOutputStream(outFile);

                byte[] buffer = new byte[1024];
                int read = 0;
                while ((read = zipInputStream.read(buffer, 0, buffer.length)) != -1) {
                    fileOutStream.write(buffer, 0, read);
                }
                fileOutStream.close();
                fileOutStream = null;
                fixtureFiles.add(outFile);
            }
        } finally {
            if (zipInputStream != null) {
                zipInputStream.close();
            }
            if (fileOutStream != null) {
                fileOutStream.close();
            }
        }
    }


    private boolean deleteFile(File file) {
        if( file.exists() ) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                for(int i=0; i<files.length; i++) {
                    if(files[i].isDirectory()) {
                        deleteFile(files[i]);
                    } else {
                        files[i].delete();
                    }
                }
            }
        }
        return(file.delete());
    }

    private void verifyNotDestroyed() {
        if (rootDir == null) {
            throw new IllegalStateException("Fixture has already been destroyed.");
        }
    }
}
