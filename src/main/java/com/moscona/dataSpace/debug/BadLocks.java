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
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Created: 1/7/11 2:23 PM
 * By: Arnon Moscona
 */
public class BadLocks {
    public static void main(String[] args) {
        try {
            String lockFile = "C:\\Users\\Admin\\projects\\intellitrade\\tmp\\bad.lock";
            FileUtils.touch(new File(lockFile));
            FileOutputStream stream = new FileOutputStream(lockFile);
//            FileInputStream stream = new FileInputStream(lockFile);
            FileChannel channel = stream.getChannel();
//            FileLock lock = channel.lock(0,Long.MAX_VALUE, true);
            FileLock lock = channel.lock();
            stream.write(new UndocumentedJava().pid().getBytes());
            stream.flush();
            long start = System.currentTimeMillis();
//            while (System.currentTimeMillis()-start < 10000) {
//                Thread.sleep(500);
//            }
            lock.release();
            stream.close();
            File f = new File(lockFile);
            System.out.println("Before write: "+FileUtils.readFileToString(f));
            FileUtils.writeStringToFile(f,"written after lock released");
            System.out.println("After write: "+FileUtils.readFileToString(f));
        }
        catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }
}
