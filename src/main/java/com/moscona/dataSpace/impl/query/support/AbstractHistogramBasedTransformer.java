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

package com.moscona.dataSpace.impl.query.support;

import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.DoubleVector;
import com.moscona.dataSpace.impl.LongVector;
import com.moscona.dataSpace.impl.segment.DoubleSegmentStats;
import com.moscona.dataSpace.impl.segment.LongSegmentStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Created: 1/4/11 12:27 PM
 * By: Arnon Moscona
 * A common base class to transformers that use and/or produce histograms
 */
public abstract class AbstractHistogramBasedTransformer<T extends IScalar>   extends AbstractVectorTransformer<T,Histogram> {
    protected HashMap<String,Integer> stringHistogram;
    protected HashMap<Long, Integer> longHistogram;
    private boolean collectBinStats = false;
    protected HashMap<Long, LongSegmentStats> longStats; // used for optional descriptive stats (int types)
    protected HashMap<Long, DoubleSegmentStats> doubleStats; // used for optional descriptive stats (float types)
    protected IVector.BaseType baseType = null;
    protected DataSpace dataSpace = null;
    protected HashMap<Long,Double> doubleValues; // holds a mapping from long representation to some real double that was used (instead of guessing from the rounded value)

    protected AbstractHistogramBasedTransformer() {
        stringHistogram = new HashMap<String,Integer>();
        longHistogram = new HashMap<Long,Integer>();
        doubleValues = new HashMap<Long, Double>();
        longStats = new HashMap<Long, LongSegmentStats>();
        doubleStats = new HashMap<Long, DoubleSegmentStats>();
    }

    protected final void setCollectBinStats(boolean collectBinStats) {
        this.collectBinStats = collectBinStats;
    }

    protected boolean shouldCollectStats() {
        return collectBinStats;
    }

    private int longHistogramCount(long value) {
        return longHistogram.containsKey(value) ? longHistogram.get(value) : 0;
    }

    private int stringHistogramCount(String value) {
        return stringHistogram.containsKey(value) ? stringHistogram.get(value) : 0;
    }

    protected ArrayList makeStringBins(ArrayList<Integer> count) throws DataSpaceException {
        ArrayList bins;
        ArrayList<String> sBins = new ArrayList<String>();
        for (String value: stringHistogram.keySet()) {
            sBins.add(value);
        }
        Collections.sort(sBins);
        for (String value: sBins) {
            count.add(stringHistogramCount(value));
        }
        bins = sBins;
        return bins;
    }

    protected ArrayList<Long> makeLongBins(ArrayList<Integer> count) throws DataSpaceException {
        ArrayList<Long> lBins = new ArrayList<Long>();
        for (Long value: longHistogram.keySet()) {
            lBins.add(value);
        }
        Collections.sort(lBins);
        for (long value: lBins) {
            count.add(longHistogramCount(value));
        }
        return lBins;
    }

    protected ArrayList<Integer> makeIntegerBins(ArrayList<Integer> count) throws DataSpaceException {
        ArrayList<Integer> iBins = new ArrayList<Integer>();
        for (Long value: longHistogram.keySet()) {
            iBins.add(value.intValue());
        }
        Collections.sort(iBins);
        for (long value: iBins) {
            count.add(longHistogramCount(value));
        }
        return iBins;
    }

    protected ArrayList<Short> makeShortBins(ArrayList<Integer> count) throws DataSpaceException {
        ArrayList<Short> shortBins = new ArrayList<Short>();
        for (Long value: longHistogram.keySet()) {
            shortBins.add(value.shortValue());
        }
        Collections.sort(shortBins);
        for (long value: shortBins) {
            count.add(longHistogramCount(value));
        }
        return shortBins;
    }

    protected ArrayList<Byte> makeByteBins(ArrayList<Integer> count) throws DataSpaceException {
        ArrayList<Byte> byteBins = new ArrayList<Byte>();
        for (Long value: longHistogram.keySet()) {
            byteBins.add(value.byteValue());
        }
        Collections.sort(byteBins);
        for (long value: byteBins) {
            count.add(longHistogramCount(value));
        }
        return byteBins;
    }

    protected ArrayList<Double> makeDoubleBins(ArrayList<Integer> count) throws DataSpaceException {
        ArrayList<Double> dBins = new ArrayList<Double>();
        for (long value: longHistogram.keySet()) {
            Double actual = doubleValues.get(value);
            dBins.add(actual);
        }
        Collections.sort(dBins);
        for (double value: dBins) {
            count.add(longHistogramCount(round(value)));
        }
        return dBins;
    }

    protected ArrayList<Float> makeFloatBins(ArrayList<Integer> count) throws DataSpaceException {
        ArrayList<Float> fBins = new ArrayList<Float>();
        for (long value: longHistogram.keySet()) {
            fBins.add(doubleValues.get(value).floatValue());
        }
        Collections.sort(fBins);
        for (float value: fBins) {
            count.add(longHistogramCount(round(value)));
        }
        return fBins;
    }

    protected ArrayList<Boolean> makeBooleanBins(ArrayList<Integer> count) throws DataSpaceException {
        ArrayList<Boolean> bBins = new ArrayList<Boolean>();
        for (Long value: longHistogram.keySet()) {
            bBins.add(value != 0L);
        }
        Collections.sort(bBins);
        for (Boolean value: bBins) {
            count.add(longHistogramCount(value?1L:0L));
        }
        return bBins;
    }

    protected void incrementLong(long bin, int count) throws DataSpaceException {
        if (longHistogram.containsKey(bin)) {
            longHistogram.put(bin, longHistogram.get(bin)+count);
        }
        else {
            longHistogram.put(bin, count);
        }
    }

    protected void incrementString(String bin, int count) throws DataSpaceException {
        if (stringHistogram.containsKey(bin)) {
            stringHistogram.put(bin, stringHistogram.get(bin)+count);
        }
        else {
            stringHistogram.put(bin,count);
        }
    }

    protected void addLongValueToStats(long bin, long value) {
        if (! longStats.containsKey(bin)) {
            longStats.put(bin, new LongSegmentStats(dataSpace.getDefaultPersistenceType()));
        }
        longStats.get(bin).add(value);
    }

    protected void addDoubleValueToStats(long bin, double value) {
        if (! doubleStats.containsKey(bin)) {
            doubleStats.put(bin, new DoubleSegmentStats(dataSpace.getDefaultPersistenceType()));
        }
        doubleStats.get(bin).add(value);
    }

    @SuppressWarnings({"unchecked", "OverlyLongMethod", "OverlyComplexMethod"})
    @Override
    protected Histogram finishTransformation() throws DataSpaceException {
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<Integer> count = new ArrayList<Integer>();
        ArrayList bins = null;

        switch (baseType) {
            case FLOAT:
                bins = makeFloatBins(count);
                break;
            case DOUBLE:
                bins = makeDoubleBins(count);
                break;
            case BYTE:
                bins = makeByteBins(count);
                break;
            case SHORT:
                bins = makeShortBins(count);
                break;
            case INTEGER:
                bins = makeIntegerBins(count);
                break;
            case LONG:
                bins = makeLongBins(count);
                break;
            case STRING:
                bins = makeStringBins(count);
                break;
            case BOOLEAN:
                bins = makeBooleanBins(count);
                break;
            default:
                throw new DataSpaceException("Don't know how to do a histogram for "+baseType);
        }

        for (Object o:bins) {
            names.add(o.toString());
        }
        Histogram histogram = new Histogram(dataSpace, names, bins, count);  // unchecked call
        if (shouldCollectStats()) {
            appendStatsColumns(histogram);
        }
        return histogram;
    }

    protected void appendStatsColumns(Histogram histogram) throws DataSpaceException {
        switch (baseType) {
            case FLOAT:
            case DOUBLE:
                appendDoubleStatsColumns(histogram);
                break;
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                appendLongStatsColumns(histogram);
                break;
            case STRING:
            case BOOLEAN:
            default:
                throw new DataSpaceException("Don't know how to do a histogram with descriptive stats for "+baseType);
        }
    }

    private void appendLongStatsColumns(Histogram histogram) throws DataSpaceException {
        ArrayList<Long> keys = new ArrayList<Long>(longStats.keySet());
        Collections.sort(keys);
        LongVector min = new LongVector(dataSpace);
        LongVector max = new LongVector(dataSpace);
        DoubleVector mean = new DoubleVector(dataSpace);
        DoubleVector stdev = new DoubleVector(dataSpace);
        for (Long key: keys) {
            min.append(longStats.get(key).getMin());
            max.append(longStats.get(key).getMax());
            mean.append(longStats.get(key).mean());
            stdev.append(longStats.get(key).stdev());
        }
        histogram.cbind(Histogram.COL_BIN_STATS_MIN, min.seal())
                .cbind(Histogram.COL_BIN_STATS_MAX, max.seal())
                .cbind(Histogram.COL_BIN_STATS_MEAN, mean.seal())
                .cbind(Histogram.COL_BIN_STATS_STDEV, stdev.seal());
    }

    private void appendDoubleStatsColumns(Histogram histogram) throws DataSpaceException {
        ArrayList<Long> keys = new ArrayList<Long>(doubleStats.keySet());
        Collections.sort(keys);
        DoubleVector min = new DoubleVector(dataSpace);
        DoubleVector max = new DoubleVector(dataSpace);
        DoubleVector mean = new DoubleVector(dataSpace);
        DoubleVector stdev = new DoubleVector(dataSpace);
        for (Long key: keys) {
            min.append(doubleStats.get(key).getMin());
            max.append(doubleStats.get(key).getMax());
            mean.append(doubleStats.get(key).mean());
            stdev.append(doubleStats.get(key).stdev());
        }
        histogram.cbind(Histogram.COL_BIN_STATS_MIN, min.seal())
                .cbind(Histogram.COL_BIN_STATS_MAX, max.seal())
                .cbind(Histogram.COL_BIN_STATS_MEAN, mean.seal())
                .cbind(Histogram.COL_BIN_STATS_STDEV, stdev.seal());
    }

    @Override
    protected void transformOne(T element, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException {
        throw new DataSpaceException("transformOne() not implemented. Should never have gotten here (supposed to do bulk transform)");
    }
}
