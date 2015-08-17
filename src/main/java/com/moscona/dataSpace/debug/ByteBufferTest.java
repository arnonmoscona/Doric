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

import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;

/**
 * Created: 1/13/11 3:02 PM
 * By: Arnon Moscona
 */
public class ByteBufferTest {
    public static void main(String[] args) {
        ByteBuffer bytes = ByteBuffer.allocate(40);
        print("after allocation",bytes);
        byte[] byteArray = bytes.array();
        for (int i=0; i<byteArray.length; i++) {
            byteArray[i] = 1;
        }
        print("after filling with 1s",bytes);
        IntBuffer ints = bytes.asIntBuffer();
        System.out.println("ints has array: "+ints.hasArray());
        int[] array = ints.array();
        for (int i=0; i<array.length; i++) {
            array[i] = 100+i;
        }
        print("after filling ints array",bytes);
    }

    public static void print(String label, ByteBuffer buf) {
        System.out.println(label+":");
        ArrayList<String> l = new ArrayList<String>();
        for (byte b: buf.array()) {
            l.add(Byte.toString(b));
        }
        System.out.println("  " + StringUtils.join(l, ", "));
    }
}
