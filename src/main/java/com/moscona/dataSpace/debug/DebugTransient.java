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


import java.io.*;

/**
 * Created: 1/6/11 3:48 PM
 * By: Arnon Moscona
 */
public class DebugTransient {
    enum Type { ONE, TWO, THREE }
    public static void main(String[] args) {
        try {
            SerializableObject obj = new SerializableObject();
            obj.print("After constructor");
            obj.unInitializedBool = true;
            obj.transientBool = true;
            obj.initializedInt = 2;
            obj.unInitializedInt = 2;
            obj.normalArray[0] = 2;
            obj.normalInitializedArray = new int[] {2,2,2,2};
            obj.transientArray = new int[1000];
            obj.transientInt = 2;
            obj.type = DebugTransient.Type.TWO;
            for (int i=0; i<obj.transientArray.length; i++) {
                obj.transientArray[i] = i;
            }
            obj.print("After changing values");
            String path = "C:/Users/Admin/Documents/tmp/tmpJavaObj.tmp";

            // write the obj out
            File file = new File(path);
            System.out.println("Writing to "+path);
            if (file.exists()) {
                file.delete();
            }
            OutputStream out = new FileOutputStream(path);
            ObjectOutput objOut = new ObjectOutputStream(out);
            objOut.writeObject(obj);
            objOut.close();
            out.close();
            System.out.println("Finished writing. "+file.length()+" bytes");


            System.out.println("Reading back.");
            FileInputStream in = new FileInputStream(path);
            ObjectInput objIn = new ObjectInputStream(in);
            SerializableObject recovered = (SerializableObject)objIn.readObject();
            objIn.close();
            in.close();
            System.out.println("finished reading.");

            recovered.print("Recovered object");
        }
        catch (Exception e) {
            System.err.println("Exception: "+e);
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @SuppressWarnings({"serial"})
    public static class SerializableObject implements Serializable {
        public int initializedInt;
        public int unInitializedInt;
        public transient int transientInt;
        public boolean initializedBool;
        public boolean unInitializedBool;
        public transient boolean transientBool;
        public int[] normalInitializedArray;
        public int[] normalArray;
        public transient int[] transientArray;
        public Type type;

        public SerializableObject() {
            initializedBool = true;
            initializedInt = 1;
            normalArray = new int[] {1,1,1,1};
        }

        public void print(String title) {
            System.out.println(title);
            System.out.println("-------------------------------------------------------");
            System.out.println("Type: "+type);
            System.out.println("initializedInt: "+initializedInt);
            System.out.println("unInitializedInt: "+unInitializedInt);
            System.out.println("transientInt: "+transientInt);
            System.out.println("initializedBool: "+initializedBool);
            System.out.println("unInitializedBool: "+unInitializedBool);
            System.out.println("transientBool: "+transientBool);
            System.out.print("normalInitializedArray: ");
            printArray(normalInitializedArray);
            System.out.print("normalArray: ");
            printArray(normalArray);
            System.out.print("transientArray: ");
            printArray(transientArray);
        }

        private void printArray(int[] arr) {
            if (arr==null) {
                System.out.println("null");
            }
            else {
                System.out.print("[");
                for (int i: arr) {
                    int limit = 10;
                    if (i< limit) {
                        System.out.print(" "+i);
                    } else {
                        if (i== limit) {
                            System.out.print("...");
                        }
                    }
                }
                System.out.println(" ]");
            }
        }
    }
}
