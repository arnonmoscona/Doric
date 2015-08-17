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
import com.moscona.dataSpace.impl.*;

import java.util.List;

/**
 * Created: 12/31/10 8:53 AM
 * By: Arnon Moscona
 * A convenience factory to convert various lists into the appropriate vectors.
 * It exists because of the erasure "feature" of java generics
 */
public class VectorFactory {
    private DataSpace dataSpace;

    public VectorFactory(DataSpace dataSpace) {
        this.dataSpace = dataSpace;
    }
    
    public IVector vector(List list) throws DataSpaceException {
        return vector(list,null);
    }

    @SuppressWarnings({"unchecked"})
    public IVector vector(List list, String name) throws DataSpaceException {
        if (list.size() == 0) {
            throw new DataSpaceException("Cannot infer member size from an empty list");
        }

        IVector retval = null;
        Class clazz = list.get(0).getClass();
        if (clazz == Byte.class) {
            retval = byteVector((List<Byte>) list);
        }
        if (clazz == Short.class) {
            retval = shortVector((List<Short>) list);
        }
        if (clazz == Integer.class) {
            retval = integerVector((List<Integer>) list);
        }
        if (clazz == Long.class) {
            retval = longVector((List<Long>) list);
        }
        if (clazz == Float.class) {
            retval = floatVector((List<Float>) list);
        }
        if (clazz == Double.class) {
            retval = doubleVector((List<Double>) list);
        }
        if (clazz == String.class) {
            retval = stringVector((List<String>) list);
        }
        if (clazz == Boolean.class) {
            retval = booleanVector((List<Boolean>) list);
        }
        if (clazz == Text.class) {
            retval = new StringVector((List<Text>)list, dataSpace);
        }
        if (clazz == Numeric.class) {
            retval = numericVector((List<Numeric>) list);
        }

        if (retval==null) {
            throw new DataSpaceException("Could not find appropriate vector type for "+clazz.getName());
        }

        assign(name,retval);

        return retval;
    }

    @SuppressWarnings({"unchecked"})
    public IVector numericVector(List list) throws DataSpaceException {
        Class clazz = ((List<Numeric>) list).get(0).getValue().getClass();
        if (clazz==Byte.class) {
            return new ByteVector((List<Numeric<Byte>>)list, dataSpace);
        }
        if (clazz==Short.class) {
            return new ShortVector((List<Numeric<Short>>)list, dataSpace);
        }
        if (clazz==Integer.class) {
            return new IntegerVector((List<Numeric<Integer>>)list, dataSpace);
        }
        if (clazz==Long.class) {
            return new LongVector((List<Numeric<Long>>)list, dataSpace);
        }
        if (clazz==Float.class) {
            return new FloatVector((List<Numeric<Float>>)list, dataSpace);
        }
        if (clazz==Double.class) {
            return new DoubleVector((List<Numeric<Double>>)list, dataSpace);
        }
        throw new DataSpaceException("Could not find appropriate vector type for Numeric<"+clazz.getSimpleName()+">");
    }

    public LogicalVector booleanVector(List<Boolean> list) throws DataSpaceException {
        LogicalVector retval = new LogicalVector(dataSpace);
        for (boolean value: list) {
            retval.append(value);
        }
        retval.seal();
        return retval;
    }

    public StringVector stringVector(List<String> list) throws DataSpaceException {
        StringVector retval = new StringVector(dataSpace);
        for (String value: list) {
            retval.append(value);
        }
        retval.seal();
        return retval;
    }

    public DoubleVector doubleVector(List<Double> list, Double resolution) throws DataSpaceException {
        DoubleVector retval = new DoubleVector(dataSpace);
        for (double value: list) {
            retval.append(value);
        }
        if (resolution!=null) {
            retval.setResolution(resolution);
        }
        retval.seal();
        return retval;
    }

    public DoubleVector doubleVector(List<Double> list) throws DataSpaceException {
        return doubleVector(list,null);
    }

    public FloatVector floatVector(List<Float> list, Double resolution) throws DataSpaceException {
        FloatVector retval = new FloatVector(dataSpace);
        for (float value: list) {
            retval.append(value);
        }
        if (resolution!=null) {
            retval.setResolution(resolution);
        }
        retval.seal();
        return retval;
    }

    public FloatVector floatVector(List<Float> list) throws DataSpaceException {
        return floatVector(list,null);
    }

    public LongVector longVector(List<Long> list) throws DataSpaceException {
        LongVector retval = new LongVector(dataSpace);
        for (long value: list) {
            retval.append(value);
        }
        retval.seal();
        return retval;
    }

    public IntegerVector integerVector(List<Integer> list) throws DataSpaceException {
        IntegerVector retval = new IntegerVector(dataSpace);
        for (int value: list) {
            retval.append(value);
        }
        retval.seal();
        return retval;
    }

    public ShortVector shortVector(List<Short> list) throws DataSpaceException {
        ShortVector retval = new ShortVector(dataSpace);
        for (short value: list) {
            retval.append(value);
        }
        retval.seal();
        return retval;
    }

    public ByteVector byteVector(List<Byte> list) throws DataSpaceException {
        ByteVector retval = new ByteVector(dataSpace);
        for (byte value: list) {
            retval.append(value);
        }
        retval.seal();
        return retval;
    }

    /**
     *
     * @param bitmap
     * @param length total desired length (optionsl) - since the bitmap size can be smaller (it stores only true values, basically) then you can pad false values up to length
     * @return
     * @throws DataSpaceException
     */
    public LogicalVector vector(IBitMap bitmap, Integer length) throws DataSpaceException {
        if (length!=null && bitmap.size()>length) {
            throw new DataSpaceException("The desired length is shorter than the given bitmap size...");
        }

        LogicalVector retval = new LogicalVector(dataSpace);
        IPositionIterator iterator = bitmap.getPositionIterator();
        int current = 0;

        while (iterator.hasNext()) {
            int pos = iterator.next();
            for (int i=current; i<pos; i++) {
                retval.append(false);
                current++;
            }
            retval.append(true);
            current++;
        }

        if (length != null) {
            for (int i=current; i<length; i++) {
                retval.append(false);
            }
        }

        retval.seal();
        return retval;
    }

    public ByteVector vector(byte[] values, String name) throws DataSpaceException {
        ByteVector retval = new ByteVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public ShortVector vector(short[] values, String name) throws DataSpaceException {
        ShortVector retval = new ShortVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public IntegerVector vector(int[] values, String name) throws DataSpaceException {
        IntegerVector retval = new IntegerVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public LongVector vector(long[] values, String name) throws DataSpaceException {
        LongVector retval = new LongVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public FloatVector vector(float[] values, String name) throws DataSpaceException {
        FloatVector retval = new FloatVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public DoubleVector vector(double[] values, String name) throws DataSpaceException {
        DoubleVector retval = new DoubleVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public LogicalVector vector(boolean[] values, String name) throws DataSpaceException {
        LogicalVector retval = new LogicalVector(values, dataSpace);
        assign(name, retval);
        return retval;
    }

    public StringVector vector(String[] values, String name) throws DataSpaceException {
        StringVector retval = new StringVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public ByteVector vector(Byte[] values, String name) throws DataSpaceException {
        ByteVector retval = new ByteVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public ShortVector vector(Short[] values, String name) throws DataSpaceException {
        ShortVector retval = new ShortVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public IntegerVector vector(Integer[] values, String name) throws DataSpaceException {
        IntegerVector retval = new IntegerVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public LongVector vector(Long[] values, String name) throws DataSpaceException {
        LongVector retval = new LongVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public FloatVector vector(Float[] values, String name) throws DataSpaceException {
        FloatVector retval = new FloatVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public DoubleVector vector(Double[] values, String name) throws DataSpaceException {
        DoubleVector retval = new DoubleVector(values, dataSpace);
        assign(name,retval);
        return retval;
    }

    public LogicalVector vector(Boolean[] values, String name) throws DataSpaceException {
        LogicalVector retval = new LogicalVector(values, dataSpace);
        assign(name, retval);
        return retval;
    }

    private void assign(String name, IVector retval) throws DataSpaceException {
        if (name!=null && ! name.trim().equals("")) {
            dataSpace.assign(name,retval);
        }
    }

    /**
     * Creates a vector of running numbers from, from+1, from+2,...,to
     * @param from
     * @param to
     * @return
     */
    public ShortVector shortCounterVector(int from, int to) throws DataSpaceException {
        ShortVector retval = new ShortVector(dataSpace);
        for (short i=(short)from; i<=to; i++) {
            retval.append(i);
        }
        retval.seal();
        return retval;
    }

    /**
     * Creates a vector of running numbers from, from+1, from+2,...,to
     * @param from
     * @param to
     * @return
     */
    public IntegerVector integerCounterVector(int from, int to) throws DataSpaceException {
        IntegerVector retval = new IntegerVector(dataSpace);
        for (int i=from; i<=to; i++) {
            retval.append(i);
        }
        retval.seal();
        return retval;
    }

    /**
     * Creates a vector of running numbers from, from+1, from+2,...,to
     * @param from
     * @param to
     * @return
     */
    public LongVector longCounterVector(long from, long to) throws DataSpaceException {
        LongVector retval = new LongVector(dataSpace);
        for (long i=from; i<=to; i++) {
            retval.append(i);
        }
        retval.seal();
        return retval;
    }
}
