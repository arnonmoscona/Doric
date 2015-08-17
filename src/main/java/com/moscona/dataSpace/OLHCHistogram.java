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

import com.moscona.util.ISimpleDescriptiveStatistic;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created: 1/20/11 11:14 AM
 * By: Arnon Moscona
 * Used for OLHC running summaries and charts
 */
public class OLHCHistogram extends Histogram<Double> {
    private static final long serialVersionUID = 2215812557834169433L;

    public static final String COL_BIN_FIRST = "binOpen";
    public static final String COL_BIN_OPEN = COL_BIN_FIRST;
    public static final String COL_BIN_LAST = "binClose";
    public static final String COL_BIN_CLOSE = COL_BIN_LAST;
    public static final String COL_BIN_MEAN = "binMean";
    public static final String COL_BIN_STDEV = "binStdev";
    public static final String COL_BIN_SUM = "sum";
    public static final String COL_BIN_SUM_SQUARES = "sumSquares";
    public static final String COL_BIN_LOW = Histogram.COL_BIN_MIN;
    public static final String COL_BIN_HIGH = Histogram.COL_BIN_MAX;

    public OLHCHistogram(DataSpace dataSpace, List<String> names, List<Double> min, List<Double> max, List<Integer> count,
                         List<Double> first, List<Double> last, List<Double> mean, List<Double>stdev,
                         List<Double> sum, List<Double>sumSquares) throws DataSpaceException {
        super(dataSpace, names, min, max, count);

        initOLHCColumns(dataSpace, names, first, last, mean, stdev, sum, sumSquares);
    }

    public OLHCHistogram(DataSpace ds, DataFrame df) throws DataSpaceException {
        super(ds);
        Set<String> columnNames = validateColumnCompatibility(df);

        for (String col: columnNames) {
            cbind(col,df.get(col));
        }
        setDescription(df.getDescription());
    }

    protected OLHCHistogram(DataSpace ds) {
        super(ds);
    }

    private Set<String> validateColumnCompatibility(DataFrame df) throws DataSpaceException {
        Set<String> columnNames = df.getColumnNames();
        String[] requiredColumns = {COL_BIN_OPEN, COL_BIN_LOW, COL_BIN_HIGH, COL_BIN_CLOSE,
                COL_BIN_FIRST, COL_BIN_LAST, COL_BIN_MIN, COL_BIN_MAX, COL_BIN_NUMBER,
                COL_BIN_STDEV, COL_BIN_SUM, COL_BIN_SUM_SQUARES, COL_COUNT};

        for (String col: requiredColumns) {
            if (!columnNames.contains(col)) {
                throw new DataSpaceException("Source data frame missing required column '"+col+"'");
            }
        }
        return columnNames;
    }

    private void initOLHCColumns(DataSpace dataSpace, List<String> names, List<Double> first,
                                 List<Double> last, List<Double> mean, List<Double> stdev,
                                 List<Double> sum, List<Double>sumSquares) throws DataSpaceException {
        int size = names.size();

        VectorFactory factory = new VectorFactory(dataSpace);
        // only work with non-zero size columns - the others were excluded by the user

        if (first.size()>0) {
            validateSize(first,"first",size);
            cbind(COL_BIN_FIRST, factory.vector(toFloats(first)));
        }
        if (last.size()>0) {
            validateSize(last,"last",size);
            cbind(COL_BIN_LAST, factory.vector(toFloats(last)));
        }
        if (mean.size()>0) {
            validateSize(mean,"mean",size);
            cbind(COL_BIN_MEAN, factory.vector(mean));
        }
        if (stdev.size()>0) {
            validateSize(stdev,"stdev",size);
            cbind(COL_BIN_STDEV, factory.vector(stdev));
        }
        if (sum.size()>0) {
            validateSize(sum,"sum",size);
            cbind(COL_BIN_SUM, factory.vector(sum));
        }
        if (sumSquares.size()>0) {
            validateSize(sumSquares,"sumSquares",size);
            cbind(COL_BIN_SUM_SQUARES, factory.vector(sumSquares));
        }

    }

    private List<Float> toFloats(List<?> source) {
        ArrayList<Float> list = new ArrayList<Float>(source.size());
        for (Object num: source) {
            list.add(((Number) num).floatValue());
        }
        return list;
    }

    public OLHCHistogram(DataSpace dataSpace, List<ISimpleDescriptiveStatistic> stats, List<Integer> periodNumbers, String[] requestedSummaries) throws DataSpaceException {
        super(dataSpace);
        // build names, min, max, count, first, last, mean, stdev lists
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<Double> min = new ArrayList<Double>();
        ArrayList<Double> max = new ArrayList<Double>();
        ArrayList<Integer> count = new ArrayList<Integer>();
        ArrayList<Double> first = new ArrayList<Double>();
        ArrayList<Double> last = new ArrayList<Double>();
        ArrayList<Double> mean = new ArrayList<Double>();
        ArrayList<Double> stdev = new ArrayList<Double>();
        ArrayList<Double> sum = new ArrayList<Double>();
        ArrayList<Double> sumSquares = new ArrayList<Double>();

        for (int period: periodNumbers) {
            names.add(Integer.toString(period));
        }

        for (ISimpleDescriptiveStatistic d: stats) {
            min.add(d.min());  // required by init() so we must include it regardless of what the user asked
            max.add(d.max());  // required by init() so we must include it regardless of what the user asked
            count.add((int)d.count()); // required by init() so we must include it regardless of what the user asked
            if (userRequested(requestedSummaries, COL_BIN_FIRST)) {
                first.add(d.first());
            }
            if (userRequested(requestedSummaries, COL_BIN_LAST)) {
                last.add(d.last());
            }
            if (userRequested(requestedSummaries, COL_BIN_MEAN)) {
                mean.add(d.mean());
            }
            if (userRequested(requestedSummaries, COL_BIN_STDEV)) {
                stdev.add(d.stdev());
            }
            if (userRequested(requestedSummaries, COL_BIN_SUM)) {
                sum.add(d.sum());
            }
            if (userRequested(requestedSummaries, COL_BIN_SUM_SQUARES)) {
                sumSquares.add(d.sumSquares());
            }
        }

        init(dataSpace, names, min, max, count);
        initOLHCColumns(dataSpace, names, first, last, mean, stdev, sum, sumSquares);
    }

    private boolean userRequested(String[] requestedSummaries, String col) {
        return requestedSummaries == null || ArrayUtils.contains(requestedSummaries, col);
    }

    public OLHCHistogram(DataSpace dataSpace, List<ISimpleDescriptiveStatistic> stats, List<Integer> periodNumbers) throws DataSpaceException {
        this(dataSpace,stats,periodNumbers,null);
    }

    public static void validateColumnName(String name) throws DataSpaceException {
        String[] validColumns = {COL_BIN_OPEN, COL_BIN_LOW, COL_BIN_HIGH, COL_BIN_CLOSE,
                COL_BIN_FIRST, COL_BIN_LAST, COL_BIN_MIN, COL_BIN_MAX, COL_BIN_NUMBER,
                COL_BIN_STDEV, COL_BIN_SUM, COL_BIN_SUM_SQUARES, COL_COUNT};
        if (!ArrayUtils.contains(validColumns, name)) {
            throw new DataSpaceException("No such standard OLHCVHistogram column: "+name);
        }
    }

    public static void validateSummaryColumnName(String name) throws DataSpaceException {
        String[] validColumns = {COL_BIN_OPEN, COL_BIN_LOW, COL_BIN_HIGH, COL_BIN_CLOSE,
                COL_BIN_FIRST, COL_BIN_LAST, COL_BIN_MIN, COL_BIN_MAX, COL_BIN_STDEV,
                COL_BIN_SUM, COL_BIN_SUM_SQUARES, COL_COUNT};
        if (!ArrayUtils.contains(validColumns, name)) {
            throw new DataSpaceException("No such standard summary OLHCVHistogram column: "+name);
        }
    }
}
