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
import com.moscona.dataSpace.impl.AbstractVector;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created: 2/14/11 2:21 PM
 * By: Arnon Moscona
 */
public class ExportHelper {
    public void csvExport(DataFrame df, String fileName, boolean includeMetaData) throws FileNotFoundException, DataSpaceException {
        // FIXME exports sorted and label columns twice - once populated and once not - the populated ones are the wrong ones
        PrintStream out = new PrintStream(new File(fileName));
        try {
            ArrayList<String> labels = new ArrayList<String>();
            ArrayList<String> sorted = new ArrayList<String>();

            for (String col: df.getColumnNames()) {
                if (df.isLabel(col)) {
                    labels.add(col);
                }
                if (df.get(col).isSorted()) {
                    sorted.add(col);
                }
            }

            if (includeMetaData) {
                csvOut(out, "name", df.getName());
                csvOut(out, "description", df.getDescription());
                csvOut(out, "row ID", df.getRowId());
                csvOut(out, "sort column", df.getSortColumn());

                Collections.sort(labels);
                Collections.sort(sorted);

                out.println(excelQuote("label columns")+","+ StringUtils.join(labels,","));
                out.println(excelQuote("sorted columns")+","+ StringUtils.join(sorted,","));
                out.println();
            }

            ArrayList<String> columns = new ArrayList<String>();
            ArrayList<String> remaining = new ArrayList<String>(df.getColumnNames());
            if (df.getRowId() != null) {
                // make first column the row ID
                String rowId = df.getRowId();
                columns.add(rowId);
                remaining.remove(rowId);
            }
            // add all the sorted columns
            columns.addAll(sorted);
            remaining.removeAll(sorted);
            remaining.removeAll(labels); // those will come in last
            Collections.sort(remaining);
            columns.addAll(remaining);
            columns.addAll(labels);

            out.println(StringUtils.join(columns,","));
            IVectorIterator<Map<String,IScalar>> iterator = df.iterator();
            while (iterator.hasNext()) {
                Map<String,IScalar> row = iterator.next();
                ArrayList<String> values = new ArrayList<String>();
                for (String col: columns) {
                    values.add(toCsvString(row.get(col)));
                }
                out.println(StringUtils.join(values,","));
            }
        }
        finally {
            out.close();
        }
    }

    public void csvExport(AbstractVector vector, String fileName, boolean includeMetaData) throws FileNotFoundException, DataSpaceException {
        PrintStream out = new PrintStream(new File(fileName));
        try {
            if (includeMetaData) {
                csvOut(out, "name", vector.getName());
                csvOut(out, "description", vector.getDescription());
                csvOut(out, "base type", vector.getBaseType().name());
                csvOut(out, "sorted", ""+vector.isSorted());
                csvOut(out, "factor", ""+vector.isFactor());

                out.println();
            }

            IVectorIterator iterator = vector.iterator();
            while (iterator.hasNext()) {
                out.println(toCsvString((IScalar) iterator.next()));
            }
        }
        finally {
            out.close();
        }
    }

    public void csvExportAll(INameSpace ns, String dirName, boolean includeMetaData) throws DataSpaceException, FileNotFoundException {
        File dir = new File(dirName);
        if (! dir.exists()) {
            dir.mkdirs();
        }

        HashMap<String,IScalar> scalars = new HashMap<String,IScalar>();

        for (String name: ns.getAssignedVariableNames()) {
            try {
                IDataElement var = ns.get(name);
                if (IScalar.class.isAssignableFrom(var.getClass())) {
                    scalars.put(name,(IScalar)var);
                }
                if (DataFrame.class.isAssignableFrom(var.getClass())) {
                    DataFrame df = (DataFrame) var;
                    csvExport(df, dirName+"/"+name+".dataFrame.csv", includeMetaData);
                }
                if (AbstractVector.class.isAssignableFrom(var.getClass())) {
                    AbstractVector vector = (AbstractVector) var;
                    csvExport(vector, dirName+"/"+name+".vector.csv", includeMetaData);
                }
            }
            catch (DataSpaceException e) {
                throw new DataSpaceException("Error while exporting "+ns.get(name).getClass().getSimpleName()+" \""+name+"\": "+e,e);
            }
        }

        if (scalars.size()==0) {
            return;
        }

        String filename = dirName+"/scalars.csv";
        PrintStream out = new PrintStream(new File(filename));
        try {
            for (String name: scalars.keySet()) {
                csvOut(out, name, toCsvString(scalars.get(name)));
            }
        }
        finally {
            out.close();
        }
    }

    private String toCsvString(IScalar value) {
        if (value==null) {
            return "";
        }
        if (Text.class.isAssignableFrom(value.getClass())) {
            return excelQuote(((Text)value).getValue());
        }
        return value.toString();
    }

    private void csvOut(PrintStream out, String key, String value) {
        out.println(excelQuote(key)+","+excelQuote(value));
    }

    public String excelQuote(String str) {
        return "\""+(str==null ? "" : str.replaceAll("\"","\"\""))+"\"";
    }
}
