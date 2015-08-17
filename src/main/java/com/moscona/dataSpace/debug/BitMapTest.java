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

import com.moscona.dataSpace.IPositionIterator;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.util.CompressedBitMap;

/**
 * Created: 12/31/10 9:41 AM
 * By: Arnon Moscona
 */
public class BitMapTest {
    public static void main(String[] args) {
        try {
            boolean[] values = {false,false,true,false,true,true,true,false,true,false,false};
            CompressedBitMap bitmap = new CompressedBitMap();
            int trueCount = 0;
            for (boolean value: values) {
                bitmap.add(value);
                if (value) {
                    trueCount++;
                }
            }
            System.out.println("value count = "+values.length);
            System.out.println("bitmap size = "+bitmap.size());
            System.out.println("true count  = "+trueCount);
            System.out.println("cardinality = "+bitmap.cardinality());
            IPositionIterator iterator = bitmap.getPositionIterator();
            System.out.println("Iterating on values...");
            for (int i=0; i<values.length; i++) {
                System.out.println("  "+ i+" : "+values[i]);
            }

            System.out.println("Iterating on positions...");
            while (iterator.hasNext()) {
                int i = iterator.next();
                System.out.println("  "+ i+" : "+values[i]);
            }
        }
        catch (DataSpaceException e) {
            System.out.println("Exception: "+e);
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
