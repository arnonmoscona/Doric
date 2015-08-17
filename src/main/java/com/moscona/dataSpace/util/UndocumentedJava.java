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

package com.moscona.dataSpace.util;

import java.lang.management.ManagementFactory;

/**
 * Created: 1/7/11 1:54 PM
 * By: Arnon Moscona
 * Access to undocumented internals.
 * Information sources:
 *   http://chaoticjava.com/posts/retrieving-a-vms-pid-and-more-info-through-java/
 */
public class UndocumentedJava {
    private static int bytesPerPointer = -1; // remembers the first calculation

    public UndocumentedJava() {

    }

    public String pid() {
        return ManagementFactory.getRuntimeMXBean().getName();
    }

    @SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod"})
    public int bytesPerPointer() {
        synchronized (UndocumentedJava.class) {
            if (bytesPerPointer==-1) {
                // important: the following is not portable and makes a serious assumption of 64bit by default. It would be better to make a heavier, more portable version. perhaps by serializing empty objects into byte arrays (e.g. make one Object, serialize and find size, then make an array of 10 Objects and find its size then see the ratio. Or maybe an empty Object[] vs Object[10]
                String prop =  System.getProperty("sun.arch.data.model");
                if (prop == null) {
                    bytesPerPointer = 8; // assume 64 bit
                }
                try {
                    bytesPerPointer = Integer.parseInt(prop)/8;
                }
                catch (Exception e) {
                    bytesPerPointer = 8; // assume 64 bit
                }
            }
        }
        return bytesPerPointer;
    }

    public boolean is64Bit() {
        return bytesPerPointer==8;
    }
}
