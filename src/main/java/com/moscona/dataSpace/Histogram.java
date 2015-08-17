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

package com.moscona.dataSpace;

import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.ShortVector;
import com.moscona.dataSpace.impl.StringVector;
import com.google.common.collect.Multiset;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created: 12/31/10 8:35 AM
 * By: Arnon Moscona
 */
public class Histogram<T extends Comparable<T>> extends DataFrame {
    public final static String COL_BIN_NUMBER = "binNumber";
    public final static String COL_NAME = "name";
    public final static String COL_BIN_MIN = "binMin";
    public final static String COL_BIN_MAX = "binMax";
    public final static String COL_COUNT = "binCount";
    public final static String COL_BIN_STATS_MIN = "binStatsMin"; // optional
    public final static String COL_BIN_STATS_MAX = "binStatsMax"; // optional
    public final static String COL_BIN_STATS_MEAN = "binStatsMean"; // optional
    public final static String COL_BIN_STATS_STDEV = "binStatsStdev"; // optional

    private static final long serialVersionUID = -6797018433507993872L;

    protected Histogram(DataSpace dataSpace) {
        super(dataSpace);
    }

    public Histogram(DataSpace dataSpace, List<String> names, List<T> min, List<T> max, List<Integer> count) throws DataSpaceException {
        super(dataSpace);
        //noinspection OverridableMethodCallDuringObjectConstruction
        init(dataSpace, names, min, max, count);
    }

    protected void init(DataSpace dataSpace, List<String> names, List<T> min, List<T> max, List<Integer> count) throws DataSpaceException {
        int size = names.size();
        validateSize(min,"min",size);
        validateSize(max,"max",size);
        validateSize(count,"count",size);

        VectorFactory factory = new VectorFactory(dataSpace);
        StringVector nameVector = factory.stringVector(names);
        IVector minVector = factory.vector(min);
        IVector maxVector = factory.vector(max);
        IVector countVector = factory.vector(count);
        ShortVector binNum = factory.shortCounterVector(0, size-1);

        cbind(COL_BIN_NUMBER, binNum);
        cbind(COL_NAME, nameVector);
        cbind(COL_BIN_MIN, minVector);
        cbind(COL_BIN_MAX, maxVector);
        cbind(COL_COUNT, countVector);
    }

    public Histogram(DataSpace dataSpace, List<String> names, List<T> bins, List<Integer> count) throws DataSpaceException {
        this(dataSpace,names,bins,bins,count);
    }

//    public Histogram(DataSpace dataSpace, List<String> names, Multiset<T> histogram) throws DataSpaceException {
//        super(dataSpace);
//        ArrayList<T> bins = new ArrayList<T>(histogram.elementSet());
//        Collections.sort(bins);
//
//        List<String> names2 = names;
//        if (names2==null) {
//            names2 = new ArrayList<String>();
//            for (T bin: bins) {
//                names2.add(bin.toString());
//            }
//        }
//
//        ArrayList<Integer> counts = new ArrayList<Integer>();
//        for (T bin: bins) {
//            counts.add(histogram.count(bin));
//        }
//        init(dataSpace,names2,bins,bins,counts);
//    }

//    public Histogram(DataSpace dataSpace, Multiset<T> histogram) throws DataSpaceException {
//        this(dataSpace,null,histogram);
//    }

    protected void validateSize(List list, String name, int size) throws DataSpaceException {
        if (list.size()!=size) {
            throw new DataSpaceException("Histogram constructor arguments must all be the same size. Expected "+size+", but \""+name+"\" was sized "+list.size());
        }
    }

    public IVector.BaseType getBaseType() throws DataSpaceException {
        return get(COL_BIN_MIN).getBaseType();
    }

    /**
     * A debug method
     */
    public void print() {
        try {
            IVectorIterator<Map<String,IScalar>> iterator = iterator();
            System.out.println("  Num\tName\tMin\tMax\tCount");
            while (iterator.hasNext()) {
                Map<String,IScalar> row = iterator.next();
                System.out.println("  "+row.get(COL_BIN_NUMBER)+"\t"+row.get(COL_NAME)+"\t"+row.get(COL_BIN_MIN)+"\t"+row.get(COL_BIN_MAX)+"\t"+row.get(COL_COUNT));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
