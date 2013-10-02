package parse.common;

import exception.ParameterNotFoundException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import parse.timestamp.CsvTimestamp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * Author: Diego Didona Email: didona@gsd.inesc-id.pt Websiste: www.cloudtm.eu Date: 24/05/12
 */
public class StatisticsContainer {


   private static final String TIMESTAMP_STRING = "Timestamp";
   Log log = LogFactory.getLog(StatisticsContainer.class);
   private Translation translation;
   private Object[][] stats;
   private int numColumns;
   private int numRows;

   public StatisticsContainer(String filePath) throws IOException {
      BufferedReader source = new BufferedReader(new FileReader(new File(filePath)));
      String header = source.readLine();
      this.translation = new Translation(header);

      this.numColumns = translation.size();
      this.numRows = getNumRows(filePath);

      Object[][] temp = new Object[numRows][numColumns];
      this.initializeStats(source, temp);
      stats = (Object[][]) temp;
      source.close();

   }

   private void initializeStats(BufferedReader br, Object[][] stats) throws IOException {
      String row;
      int i = 0;
      while ((row = br.readLine()) != null) {
         fillRow(stats, i, row);
         i++;
      }
      //dump((Double[][])stats,stats.length,stats[0].length);
   }

   public boolean containsParam(String param) {
      return translation.exist(param);
   }

   private void fillRow(Object[][] stats, int i, String row) {
      String[] split = row.split(",");
      int j = 0;
      for (String s : split) {
         try {
            stats[i][j] = (Object) s;
         } catch (Exception e) {
            log.warn("Trying to parse " + s + ". Putting -1");
            stats[i][j] = -1;
         }
         j++;
      }
   }

   private int getNumRows(String f) throws IOException {
      int i = 0;
      BufferedReader br = new BufferedReader(new FileReader(new File(f)));
      while (br.readLine() != null)
         i++;
      return i - 1; //do not consider header!
   }

   public double[] getParam(String param) throws ParameterNotFoundException {
      int index = this.translation.getParamIndex(param);
      return getColumn(index, 0, numRows - 1);
   }

   public double[] getParam(String param, CsvTimestamp from, CsvTimestamp to) throws ParameterNotFoundException {
      int indexCol = this.translation.getParamIndex(param);
      int[] indexTimestamp = getIndexTimeStamps(from, to);
      return getColumn(indexCol, indexTimestamp[0], indexTimestamp[1]);
   }

   public String getStrParam(String param, CsvTimestamp from, CsvTimestamp to) throws ParameterNotFoundException {
      int indexCol = this.translation.getParamIndex(param);
      int[] indexTimestamp = getIndexTimeStamps(from, to);
      return getValue(indexCol, indexTimestamp[0], indexTimestamp[1]);
   }

   public String getStrParam(String param) throws ParameterNotFoundException {
      int indexCol = this.translation.getParamIndex(param);
      return getValue(indexCol, 0, numRows - 1);
   }

   private double[] getColumn(int indexColumn, int indexFrom, int indexTO) {
      double[] res = new double[indexTO - indexFrom + 1];
      for (int i = 0, j = indexFrom; j <= indexTO; j++, i++)
         res[i] = new Double((String) stats[j][indexColumn]);
      return res;
   }

   private String getValue(int indexColumn, int indexFrom, int indexTO) {
      return (String) stats[indexFrom][indexColumn];
   }

   /* Not really optimized...*/
   private int[] getIndexTimeStamps(CsvTimestamp from, CsvTimestamp to) {
      final boolean trace = log.isTraceEnabled();
      int[] res = new int[2]; //0: from ; 1: to
      int timestampIndex = translation.getParamIndex(TIMESTAMP_STRING);
      if (trace) {
         log.trace("Searching for index of [" + from.toLong() + ", " + to.toLong() + "] with numRows = " + numRows);
      }
      Long fromStr = from.toLong();
      Long toStr = to.toLong();
      boolean lowerBoundSet = false;

      for (int i = 0; i < numRows; i++) {
         Long temp = new Long((String) stats[i][timestampIndex]);
         if(trace) log.trace("Comparing "+temp);
            if (!lowerBoundSet && temp >= fromStr) {
               res[0] = i;
               lowerBoundSet = true;
            }
         if (temp >= toStr) {
            res[1] = i;
            if (trace) log.trace("Returning boundaries " + Arrays.toString(res));
            return res;
         }
      }
      if (trace) log.trace("Returning boundaries " + Arrays.toString(res));
      return res;
   }

   @SuppressWarnings("unused")
   private void dump(Double[][] a, int row, int col) {
      for (int i = 0; i < row; i++) {
         for (int j = 0; j < col; j++)
            System.out.print(a[i][j] + ",");
         System.out.print("\n");
      }
   }


}
