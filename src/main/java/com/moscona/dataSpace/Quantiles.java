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

import java.util.Arrays;

/**
 * Created: 12/29/10 2:04 PM
 * By: Arnon Moscona
 * A class that represents a standard set of quantiles
 */
public class Quantiles implements IQuantiles {

    private static final long serialVersionUID = 3839935060586016183L;
    public static final int MARKER_COUNT = 21;
    private Double[] percentiles;
    private long[] markerMoves; // counts the number of times each marker was adjusted in estimation
    private double markerMoveHerfindahlIndex = 0.0;
    private double minMarkerMoveHerfindahlIndex = 0.0;

    private Form form;
    private long numObservations;

    public Quantiles() {
        percentiles = new Double[MARKER_COUNT];
        markerMoves = new long[MARKER_COUNT];
        form = null;
    }

    /**
     * Creates a quantiles object using a TEMPORARY array the caller provides. This array MAY BE SORTED and the
     * constructor makes NO PROMISE as to the condition of the array after the call.
     * @param values
     * @throws DataSpaceException
     */
    public Quantiles(double[] values) throws DataSpaceException {
        this();
        int size = values.length;
        if (size==0) {
            return;
        }

        if (size <= 5) {
            calculateMedian(values);
        }
        else if (size <= 20) {
            calculateFourQuantiles(values);
        }
        else {
            calculatePreciseQuantiles(values);
        }
    }

    @Override
    public Double getPercentile(int percentile) throws DataSpaceException {
        validatePercentile(percentile);
        return percentiles[percentile/5];
    }

    public void setPercentile(int percentile, double value) throws DataSpaceException {
        validatePercentile(percentile);
        percentiles[percentile/5] = value;
    }

    private void validatePercentile(int percentile) throws DataSpaceException {
        if (percentile < 0 || percentile > 100) {
            throw new DataSpaceException("percentile must be in the range of 0-100");
        }
        if (percentile%5 != 0) {
            throw new DataSpaceException("Percentile must be one of the standard 0,5,10,...,95,100");
        }
    }

    @Override
    public Double median() {
        return percentiles[10];
    }

    @Override
    public Form getForm() {
        return form;
    }

    public void setForm(Form form) {
        this.form = form;
    }

    // HOLD (fix before release)  this is a very dumb estimation algorithm and breaks badly under well defined conditions. Knowing that this is temporary, the implementation is rather sloppy. Before production we must fix this. See IT-464

    public void startEstimation(int segmentSize) {
        numObservations = segmentSize;
    }

    /**
     * This estimation algorithm is inspired by p-square and similar algorithm, but uses linear estimates, and is
     * probably not as good, but I fully understand it. I think it will work because:
     *   - we are starting with a precise calculation on a rather large sample (about a million usually)
     *   - in most cases we assume that the starting point is already pretty accurate
     *   - in our particular case, we try to work with bounded metrics to extreme outliers are unlikely
     *   - we are generally not trying to split hairs
     * @param observation
     */
    public void addObservationToQuantileEstimate(double observation) {
        int lowerIndex = findLowerPointToBeAdjusted(observation); // find the highest point where the value clearly lies between it and the next point
        if (lowerIndex<=0) {
            adjustMinBin(observation); // special case
        }
        else if (lowerIndex>=percentiles.length-2) {
            adjustMaxBin(observation); // special case
        }
        else {
            // OK, we now have a lower index that is not the first, nor the last bin
            long n = numObservations;
            numObservations++;

            double p1 = percentiles[lowerIndex-1];
            double p2 = percentiles[lowerIndex]; // p1..p2 is the interval of the bin below the lower percentile. The middle of this is m1
            double p3 = percentiles[lowerIndex+1]; // p2..p3 is the bin in which the new observation lies (the one whose upper and lower boundaries we are adjusting). The Middle of this is m2
            double p4 = percentiles[lowerIndex+2]; // p3..p4 is the bin above the higher percentile. The middle of this is m3

            // first check whether the new observation is sufficiently far enough from both boundaries to even warrant an adjustment
            double epsilon = (p4-p1)/100000;

            // then do a crude "vote" among all the existing points contributing to the bins
            // calculate the average number of points per bin (weight: W)
            double w = ((double)n)/(percentiles.length-1);

            // The lower point sits between two bins. The lower bin has a rough mid-point as does the higher bin
            double m1 = (p1+p2)/2.0;
            double m2 = (p2+p3)/2.0;
            double m3 = (p3+p4)/2.0;

            // make the new estimate L1 = (W*midLowerBin + W*midUpperBin + observation)/(2W+1)
            double l1 = (w*(m1+m2)+observation)/(2*w+1);
            // repeat the same estimation procedure for the higher estimate
            double h1 = (w*(m2+m3)+observation)/(2*w+1);

            // check whether the new lower estimate is still higher than the previous percentile estimate for the lower point. If so, move the old estimate to the new estimate. Do not allow the estimate to jump above the next percentile boundary
            if (p3>l1 && l1-p2 > epsilon) {
                markerMoves[lowerIndex]++;
                percentiles[lowerIndex] = l1;
            }
            // check whether the new higher estimate is still lower than the previous percentile estimate for the higher point. If so, move the old estimate to the new estimate. Do not allow the new estimate to drop below the new lower percentile boundary.
            if (h1>l1 && p3-h1 > epsilon) {
                markerMoves[lowerIndex+1]++;
                percentiles[lowerIndex+1] = h1;
            }
        }

    }

    /**
     * Finds an index in the range -1..MARKER_COUNT where percentiles[i] <= observation && percentiles[i+1] > observations
     * @param observation
     * @return
     */
    private int findLowerPointToBeAdjusted(double observation) {
        if (observation < percentiles[0]) {
            return -1;
        }
        if (observation >= percentiles[percentiles.length-1]) {
            return percentiles.length;
        }
        for (int i=0; i<percentiles.length-1; i++) {
            if (observation >= percentiles[i] && observation < percentiles[i+1]) {
                return i;
            }
        }
        return percentiles.length; // should never really get here
    }

    private void adjustMaxBin(double observation) {
        // adjust the total number of observations
        double n = numObservations;
        numObservations++;

        // move the max marker to the new observation if needed
        int last = percentiles.length - 1;
        if (observation>percentiles[last]) {
            markerMoves[last]++;
            percentiles[last] = observation;
        }

        // move the last marker (bottom of bin) by voting between the new observation and the assumed number of observations in the bin
        double w = n /(percentiles.length-1);
        percentiles[last-1] = (w*percentiles[last-1]+observation)/(w+1);
        markerMoves[last-1]++;
    }

    private void adjustMinBin(double observation) {
        // adjust the total number of observations
        double n = numObservations;
        numObservations++;

        // move the min marker to the new observation if needed
        if (observation<percentiles[0]) {
            markerMoves[0]++;
            percentiles[0] = observation;
        }

        // move the first marker (top of bin) by voting between the new observation and the assumed number of observations in the bin
        double w = n /(percentiles.length-1);
        percentiles[1] = (w*percentiles[1]+observation)/(w+1);
        markerMoves[1]++;
    }

    public void finishEstimation() throws DataSpaceException {
        long sum = 0L;
        for (long i: markerMoves) {
            sum+=i;
        }
        if (sum==0) {
            return;
        }
        double pct[] = new double[markerMoves.length];
        for (int i=0; i<markerMoves.length; i++) {
            pct[i] = ((double)markerMoves[i])/sum;
        }

        double index = 0.0;
        for (double p:pct) {
            index += p*p;
        }
        markerMoveHerfindahlIndex = index;
        minMarkerMoveHerfindahlIndex = 1.0/markerMoves.length;
    }

    public long[] getMarkerMoves() {
        return Arrays.copyOf(markerMoves, markerMoves.length);
    }

    public double getMinMarkerMoveHerfindahlIndex() {
        return minMarkerMoveHerfindahlIndex;
    }

    public double getMarkerMoveHerfindahlIndex() {
        return markerMoveHerfindahlIndex;
    }

    public long getNumObservations() {
        return numObservations;
    }

    /**
     * For very short vectors we directly calculate the median only
     * @return
     */
    private void calculateMedian(double[] values) throws DataSpaceException {
        Arrays.sort(values);
        int middleIndex = values.length / 2;
        double median = values[middleIndex];
        if (values.length%2 == 0) {
            median = (median + values[middleIndex-1])/2;
        }

        setForm(Quantiles.Form.MEDIAN_ONLY);
        setPercentile(50, median);

        setPercentile(0,values[0]);
        setPercentile(100,values[values.length-1]);
    }


    private void calculateFourQuantiles(double[] values) throws DataSpaceException {
        Arrays.sort(values);
        int middleIndex = values.length / 2;
        double median = values[middleIndex];
        if (values.length%2 == 0) {
            median = (median + values[middleIndex-1])/2;
        }

        setForm(Quantiles.Form.FOUR_QUANTILES);
        setPercentile(50, median);

        int twentyFifth = values.length / 4;
        setPercentile(25, values[twentyFifth]);
        int seventyFifth = (values.length * 3)/4;
        setPercentile(75, values[seventyFifth]);

        setPercentile(0,values[0]);
        setPercentile(100,values[values.length-1]);
    }

    private void calculatePreciseQuantiles(double[] values) throws DataSpaceException {
        Arrays.sort(values);
        setForm(Quantiles.Form.FIVE_PERCENTILE_BINS);

        setPercentile(0,values[0]);
        for (int  i=1; i<20; i++) {
            int index = (int)Math.round(((double)values.length * i) / 20-1);
            setPercentile(i * 5, values[index]);
        }
        setPercentile(100,values[values.length-1]);
    }
}
