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

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Created: 12/31/10 11:08 AM
 * By: Arnon Moscona
 */
public class PriorityQueueTest {
    public static void main(String[] args) {
        PriorityQueue<Integer> topN = new PriorityQueue<Integer>(5, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1); // reverse
            }
        });
        int[] values = {7,6,5,4,1,2,3,7,8,9,5,8,9};
        int n = 4;
        for (int value: values) {
            if (! topN.contains(value)) {
                topN.add(value);
            }
            if (topN.size()>n) {
                topN.poll();
            }
        }
        Iterator<Integer> iterator = topN.iterator();
        System.out.println("Queue size "+topN.size());
        System.out.println("Top "+n+":");
        System.out.println("peek() "+topN.peek());
        int size = topN.size();
        for (int i=0; i<Math.min(n,size); i++) {
            System.out.println("  "+i+": "+topN.poll());
        }
    }
}
