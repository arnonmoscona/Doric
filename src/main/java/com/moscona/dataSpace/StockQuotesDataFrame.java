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

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.util.ISimpleDescriptiveStatistic;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.*;
import com.moscona.dataSpace.impl.query.EqualsQuery;
import com.moscona.dataSpace.impl.query.RunningOLHCSummaryHistogramQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created: 1/24/11 10:37 AM
 * By: Arnon Moscona
 */
public class StockQuotesDataFrame extends OLHCHistogram {
    private static final long serialVersionUID = 4248417408977763654L;

    public static final String COL_BIN_SYMBOL = COL_NAME;
    public static final String COL_BIN_VOLUME = "binVolume";
    public static final String COL_SYMBOL = "symbol";
    public static final String COL_BIN_CLOSING_SECOND = "second";
    public static final double PRICE_RESOLUTION = 0.005;

    public StockQuotesDataFrame(DataSpace dataSpace, List<String> names, List<Double> min, List<Double> max, List<Integer> count, List<Double> first, List<Double> last, List<Double> mean, List<Double> stdev, List<Double> sum, List<Double> sumSquares) throws DataSpaceException {
        super(dataSpace, names, min, max, count, first, last, mean, stdev, sum, sumSquares);
    }

    public StockQuotesDataFrame(DataSpace dataSpace, List<ISimpleDescriptiveStatistic> stats, List<Integer> periodNumbers) throws DataSpaceException {
        super(dataSpace, stats, periodNumbers);
    }

    /**
     * Creates a OLHCVHistogram from a vector compatible data frame
     * @param df
     */
    public StockQuotesDataFrame(DataSpace ds, DataFrame df) throws DataSpaceException {
        super(ds,df);
        if (df.getColumnNames().contains(COL_BIN_VOLUME)) {
            cbind(COL_BIN_VOLUME, df.get(COL_BIN_VOLUME));
        }
    }

    public StockQuotesDataFrame(DataSpace dataSpace, String description, List<Integer> second, List<String> symbol,
                                List<Float> open, List<Float> low, List<Float> high, List<Float> close,
                                List<Long> volume) throws DataSpaceException {
        super(dataSpace);

        VectorFactory factory = new VectorFactory(dataSpace);
        IntegerVector secondsColumn = factory.integerVector(second);
        secondsColumn.setDescription("seconds from start of trading (period closing second)");
        cbind(COL_BIN_CLOSING_SECOND, secondsColumn);

        StringVector symbolColumn = factory.stringVector(symbol);
        symbolColumn.setDescription("symbol");
        cbind(COL_SYMBOL, symbolColumn);

        FloatVector openColumn = factory.floatVector(open, PRICE_RESOLUTION);
        openColumn.setDescription("open price for period");
        cbind(COL_BIN_OPEN, openColumn);

        FloatVector lowColumn = factory.floatVector(low, PRICE_RESOLUTION);
        lowColumn.setDescription("low price for period");
        cbind(COL_BIN_LOW, lowColumn);

        FloatVector highColumn = factory.floatVector(high, PRICE_RESOLUTION);
        highColumn.setDescription("high price for period");
        cbind(COL_BIN_HIGH, highColumn);

        FloatVector closeColumn = factory.floatVector(close, PRICE_RESOLUTION);
        closeColumn.setDescription("close price for period");
        cbind(COL_BIN_CLOSE, closeColumn);

        LongVector volumeColumn = factory.longVector(volume);
        volumeColumn.setDescription("total volume for period");
        cbind(COL_BIN_VOLUME, volumeColumn);

        setDescription(description);
    }

//    public static void validateColumnName(String name) throws DataSpaceException {
//        String[] validColumns = {COL_BIN_OPEN, COL_BIN_LOW, COL_BIN_HIGH, COL_BIN_CLOSE, COL_BIN_SYMBOL, COL_BIN_VOLUME,
//        COL_BIN_FIRST, COL_BIN_LAST, COL_BIN_MIN, COL_BIN_MAX, COL_BIN_NUMBER, COL_BIN_STDEV, COL_BIN_SUM, COL_BIN_SUM_SQUARES, COL_COUNT};
//        if (!ArrayUtils.contains(validColumns, name)) {
//            throw new DataSpaceException("No such standard OLHCVHistogram column: "+name);
//        }
//    }
//
//    public static void validateSummaryColumnName(String name) throws DataSpaceException {
//        String[] validColumns = {COL_BIN_OPEN, COL_BIN_LOW, COL_BIN_HIGH, COL_BIN_CLOSE, COL_BIN_VOLUME,
//                COL_BIN_FIRST, COL_BIN_LAST, COL_BIN_MIN, COL_BIN_MAX, COL_BIN_NUMBER, COL_BIN_STDEV, COL_BIN_SUM,
//                COL_BIN_SUM_SQUARES, COL_COUNT};
//        if (!ArrayUtils.contains(validColumns, name)) {
//            throw new DataSpaceException("No such standard summary OLHCVHistogram column: "+name);
//        }
//    }

    @SuppressWarnings({"unchecked"}) // unchecked casts of column types
    public StockQuotesDataFrame(DataSpace ds, DataFrame source, int binSize, String description, ArrayList<String> sortedSignals, Map<String,Float> openingPrices) throws DataSpaceException, InvalidArgumentException {
        super(ds);
        String symbolColumn = COL_SYMBOL;
        if (!source.getColumnNames().contains(COL_SYMBOL)) {
            symbolColumn = COL_BIN_SYMBOL;
        }

        String[] requiredColumns = {symbolColumn, COL_BIN_OPEN, COL_BIN_LOW, COL_BIN_HIGH, COL_BIN_CLOSE, COL_BIN_VOLUME, COL_BIN_CLOSING_SECOND};
        for (String col: requiredColumns) {
            if (!source.getColumnNames().contains(col)) {
                throw new InvalidArgumentException("Source data frame missing required column '"+col+"'");
            }
        }

        // create and document result vectors
        StringVector symbol = new StringVector(ds);
        FloatVector open = new FloatVector(ds);
        FloatVector low = new FloatVector(ds);
        FloatVector high = new FloatVector(ds);
        FloatVector close = new FloatVector(ds);
        IntegerVector volume = new IntegerVector(ds);
        IntegerVector second = new IntegerVector(ds);


        int counter = 0;
        long start = System.currentTimeMillis();

        for (String s: sortedSignals) {
            // populate opening prices (before trading starts)
            symbol.append(s);
            Float price = openingPrices.get(s);
            open.append(price);
            low.append(price);
            high.append(price);
            close.append(price);
            volume.append(0);
            second.append(0);

            // create a bitmap from the source symbol column (equals query)
            EqualsQuery<Text> symbolQuery = new EqualsQuery<Text>();
            IQueryParameterList params = symbolQuery.createParameterList(symbol.getBaseType()).set(EqualsQuery.VALUE, s); // HOLD (fix before release)  there has to be a nicer way to do it. Maybe symbol.equals(s) => IBitMap or new VectorQueryFacade(symbol).equals(s)  or vector.query("=",value)
            QueryState queryState = new QueryState();
            IBitMap symbolBitMap = symbolQuery.apply(params, source.get(symbolColumn), queryState);

            // generate a running summary for each of the columns, asking for the appropriate summary
            int summarySize = populateSymbolFloatSummary(source, COL_BIN_OPEN, symbolBitMap, binSize, open, COL_BIN_FIRST, queryState);
            populateSymbolFloatSummary(source, COL_BIN_LOW, symbolBitMap, binSize, low, COL_BIN_LOW, queryState);
            populateSymbolFloatSummary(source, COL_BIN_HIGH, symbolBitMap, binSize, high, COL_BIN_HIGH, queryState);
            populateSymbolFloatSummary(source, COL_BIN_CLOSE, symbolBitMap, binSize, close, COL_BIN_LAST, queryState);
            populateSymbolIntegerSummary(source, COL_BIN_VOLUME, symbolBitMap, binSize, volume, COL_BIN_SUM, queryState);
            populateSymbolIntegerSummary(source, COL_BIN_CLOSING_SECOND, symbolBitMap, binSize, second, COL_BIN_MAX, queryState);

            // create a fixed value vector N*symbol and append it to the symbol vector
            symbol.append(s, summarySize);
        }

        // assemble the data frame
        sealAndBind(COL_SYMBOL, symbol);
        sealAndBind(COL_BIN_OPEN, open);
        sealAndBind(COL_BIN_LOW, low);
        sealAndBind(COL_BIN_HIGH, high);
        sealAndBind(COL_BIN_CLOSE, close);
        sealAndBind(COL_BIN_VOLUME, volume);
        sealAndBind(COL_BIN_CLOSING_SECOND, second);

        setDescription(description);
    }
    
    private void sealAndBind(String name, AbstractVector vector) throws DataSpaceException {
        vector.seal();
        cbind(name,vector);
    }

    @SuppressWarnings({"unchecked"}) // unchecked casts of column types
    private int populateSymbolFloatSummary(DataFrame source, String sourceColumn, IBitMap selection, int binSize,
                                            FloatVector resultColumn, String requestedSummary,
                                            QueryState queryState) throws DataSpaceException, InvalidArgumentException {
        RunningOLHCSummaryHistogramQuery<Numeric<Float>> transformer = new RunningOLHCSummaryHistogramQuery<Numeric<Float>>(binSize, selection, new String[] {requestedSummary});
        // HOLD #IT-493 iefficient use of temporary histogram leads to long query times (0.45 sec/symbol)
        Histogram openHistogram = transformer.transform(source.get(sourceColumn), queryState);
        IVector summary = openHistogram.get(requestedSummary);
        resultColumn.append(summary);
        return summary.size();
    }

    @SuppressWarnings({"unchecked"}) // unchecked casts of column types
    private int populateSymbolIntegerSummary(DataFrame source, String sourceColumn, IBitMap selection, int binSize,
                                            IntegerVector resultColumn, String requestedSummary,
                                            QueryState queryState) throws DataSpaceException, InvalidArgumentException {
        RunningOLHCSummaryHistogramQuery<Numeric<Float>> transformer = new RunningOLHCSummaryHistogramQuery<Numeric<Float>>(binSize, selection, new String[] {requestedSummary});
        Histogram openHistogram = transformer.transform(source.get(sourceColumn), queryState);
        IVector summary = openHistogram.get(requestedSummary);
        resultColumn.append(summary);
        return summary.size();
    }
}
