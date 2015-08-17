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

import javaewah.EWAHCompressedBitmap;
import javaewah.IntIterator;

import java.util.Iterator;
import java.util.Vector;

/**
 * Created: 2/4/11 6:03 PM
 * By: Arnon Moscona
 */
public class EwahIteratorProblem {
    public static void main(String[] args) {
        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        for (int i=9434560; i<=9435159; i++) {
            bitmap.set(i);
        }
        IntIterator iterator = bitmap.intIterator();
        System.out.println("hasNext: "+iterator.hasNext());
        System.out.println("next:    "+iterator.next());
        System.out.println("first:   "+bitmap.getPositions().get(0));
        Iterator<Integer> v = bitmap.getPositions().iterator();
        System.out.println("using the positions vector iterator:");
        System.out.println("hasNext: "+v.hasNext());
        System.out.println("next:    "+v.next());
    }
}
