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

package com.moscona.dataSpace.debug;

import com.moscona.dataSpace.DataSpace;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.StringVector;
import com.moscona.dataSpace.stub.DataStore;
import com.moscona.dataSpace.stub.MemoryManager;

import java.util.regex.Pattern;

/**
 * Created: 12/14/10 11:16 AM
 * By: Arnon Moscona
 */
public class TestConstruction {
    public static void main(String[] args) {
        try {
            MemoryManager memoryManager = new MemoryManager();
            DataStore dataStore = new DataStore();
            DataSpace dataSpace = new DataSpace(dataStore, memoryManager);
            StringVector vector = new StringVector(dataSpace);
            Pattern namePattern = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]+");
            System.out.println("pattern: /"+namePattern+"/");
            System.out.println("'MyVar2' matches? "+namePattern.matcher("myVar2").matches());
            System.out.println("'My Var' matches? "+namePattern.matcher("My Var").matches());
            String name = TestConstruction.class.getSimpleName();
            System.out.println("name = "+name);

            int[] a = {1,2,3};
            int[] b = a;
            a[0] = 9;
            System.out.println("should be 9: "+b[0]);
        }
        catch (DataSpaceException e) {
            System.err.println("Exception: "+e);
            e.printStackTrace(System.err);  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
