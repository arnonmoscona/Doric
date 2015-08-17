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

import com.moscona.dataSpace.util.UndocumentedJava;

import java.io.*;

/**
 * Created: 1/13/11 12:43 PM
 * By: Arnon Moscona
 */
public class JvmBits {
    public static void main(String[] args) {
        try {
            System.out.println("bytes/pointer: "+(new UndocumentedJava().bytesPerPointer()));
            serializeAndPrint("empty array", new ObjectArray(0, false));
            serializeAndPrint("10 Object array, not populated", new ObjectArray(10,false));
            serializeAndPrint("10 Object array, populated", new ObjectArray(10,true));
            serializeAndPrint("empty string array", new StringArray(0,false,false));
            serializeAndPrint("10 string array, not populated", new StringArray(10,false,false));
            serializeAndPrint("10 string array, populated with empty strings", new StringArray(10,true,false));
            serializeAndPrint("10 string array, populated with random strings", new StringArray(10,true,true));
            serializeAndPrint("1000 string array, not populated", new StringArray(1000,false,false));
            serializeAndPrint("1000 string array, populated with empty strings", new StringArray(1000,true,false));
            serializeAndPrint("1000 string array, populated with random strings", new StringArray(1000,true,true));   
            serializeAndPrint("empty Long array", new LongArray(0,false));
            serializeAndPrint("10 Long array, not populated", new LongArray(10,false));
            serializeAndPrint("10 Long array, populated with 0 Longs", new LongArray(10,true));
            serializeAndPrint("1000 Long array, not populated", new LongArray(1000,false));
            serializeAndPrint("1000 Long array, populated with 0 Longs", new LongArray(1000,true));
        }
        catch (IOException e) {
            System.err.println("Exception: "+e);
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static void serializeAndPrint(String label, Serializable obj) throws IOException {
        // Serialize to a byte array
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutputStream out = new ObjectOutputStream(bos) ;
        out.writeObject(obj);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        int size = buf.length;

        System.out.println(label+": "+size);
    }

    @SuppressWarnings({"serial"})
    public static class ObjectArray implements Serializable {
        private SerializableObject[] objects;
        public ObjectArray(int size, boolean populate) {
            objects = new SerializableObject[size];
            if (populate) {
                for (int i=0; i<size; i++) {
                    objects[i] = new SerializableObject();
                }
            }
        }
    }

    @SuppressWarnings({"serial"})
    public static class StringArray implements Serializable {
        private String[] objects;
        public StringArray(int size, boolean populate, boolean useRandom) {
            objects = new String[size];
            if (populate) {
                for (int i=0; i<size; i++) {
                    objects[i] = useRandom ? randomString(i) : "";
                }
            }
        }

        private String randomString(int i) {
            if (i<5)
                return Double.toString(Math.random());
            else
                return objects[i%5];
        }
    }  

    @SuppressWarnings({"serial"})
    public static class LongArray implements Serializable {
        private long[] objects;
        public LongArray(int size, boolean populate) {
            objects = new long[size];
            if (populate) {
                for (int i=0; i<size; i++) {
                    objects[i] = 0L;
                }
            }
        }
    }

    @SuppressWarnings({"serial"})
    public static class SerializableObject implements Serializable {

    }
}
