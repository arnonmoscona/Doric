/*
 * Copyright (c) 2015. Arnon Moscona
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.moscona.test.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

/**
 * Created by Arnon on 5/6/2014.
 * This is a temporary, and not very clean solution for a problem I have with standardising test fixture locations etc.
 * It relies on placing a "marker file" somewhere above the class file in the folder hierarchy.
 * It starts out being intended for "exploded" projects not in a JAR. Since this class itself would be in a JAR, it
 * cannot be used as the root of the search. the caller must identify a class that would be used for the file search
 * from the project being tested, as the search starts from finding a resource.
 */
public class TestResourceHelper {
    public static final int MAX_SEARCH_DEPTH = 20;
    public static final String DEFAULT_ROOT_MARKER = "projectRootMaker";

    private String projectRootPath = null;

    public TestResourceHelper() throws IOException {
        this(DEFAULT_ROOT_MARKER);
    }

    public TestResourceHelper(String resource) throws IOException {
        this(resource, (String)null);
    }

    public TestResourceHelper(String resource, URL startingUrl) throws IOException {
        init(resource, startingUrl);
    }

    private void init(String resource, URL url) throws IOException {
        File resourceDir = new File(url.getFile()).getParentFile();

        int depthCharge = MAX_SEARCH_DEPTH;
        File candidate = makeNewCandidate(resource, resourceDir);
        while (depthCharge-- > 0 && !candidate.exists()) {
            resourceDir = resourceDir.getParentFile();
            candidate = makeNewCandidate(resource, resourceDir);
        }
        if (depthCharge <= 0) {
            throw makeException(resource);
        }

        projectRootPath = resourceDir.getPath();
    }

    public TestResourceHelper(String resource, String anchorClassName) throws IOException {
        URL url = this.getClass().getResource(this.getClass().getSimpleName() + ".class");
        if (anchorClassName != null) {
            url = this.getClass().getResource(anchorClassName + ".class");
        }
        init(resource, url);
    }

    private FileNotFoundException makeException(String resource) {
        return new FileNotFoundException("could not find root marker file using \"" + resource + "\"");
    }

    private File makeNewCandidate(String resource, File resourceDir) throws IOException {
        if (resourceDir == null) {
            throw makeException(resource);
        }
        return new File(resourceDir.getCanonicalPath() + "/" + resource);
    }

    public String getProjectRootPath() {
        return projectRootPath;
    }
}
