package parse.common;

import exception.ParameterNotFoundException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Author: Diego Didona Email: didona@gsd.inesc-id.pt Websiste: www.cloudtm.eu Date: 24/05/12
 */
public abstract class CsvParser implements CsvParser_I {

   protected final static String NUMBER_OF_NODES = "NumNodes";


   static {
      System.out.println("Parsing framework: only Sum and Avg params cope with partial replication.");
   }

   protected StatisticsContainer stats;
   protected Log log = LogFactory.getLog(CsvParser.class);
   protected String relevantPath;

   public CsvParser(String path) throws IOException {
      relevantPath = path;
      stats = new StatisticsContainer(path);
   }

   public double getAvgParam(String param) {
      double[] values;
      try {
         values = paramToArray(param);
      } catch (ParameterNotFoundException p) {
         log.warn("Parameter " + p + " not found. Returning -1");
         return -1;
      }
      int i = 1;
      for (; i < values.length; i++) {
         values[i] += values[i - 1];
      }
      return values[i - 1] / (i);

   }


   public double getSumParam(String param) {
      double[] values;
      try {
         values = paramToArray(param);
      } catch (ParameterNotFoundException p) {
         log.warn("Parameter " + p + " not found. Returning -1");
         return -1;
      }
      int i = 1;
      for (; i < values.length; i++) {
         values[i] += values[i - 1];
      }
      return values[i - 1];
   }

   public double getStdDev(String param) {
      double mean = getAvgParam(param);
      double[] params = paramToArray(param);
      double std = 0;
      double num = params.length;
      for (double d : params)
         std += (d - mean) * (d - mean);
      std /= num;
      return Math.pow(std, .5D);
   }

   public double getMax(String param) {
      double params[] = paramToArray(param);
      double max = Double.MIN_VALUE;
      for (double d : params)
         max = Math.max(max, d);
      return max;
   }

   public double getMin(String param) {
      double params[] = paramToArray(param);
      double min = Double.MAX_VALUE;
      for (double d : params)
         min = Math.min(min, d);
      return min;
   }

   public double[] getParam(String param) {
      return paramToArray(param);
   }

   protected double[] paramToArray(String param) {
      try {
         return this.stats.getParam(param);
      } catch (ParameterNotFoundException p) {
         return this.stats.getParam(paramFirstLowerCase(param));
      }
   }

   public final boolean isParam(String param) {
      boolean b = stats.containsParam(param);
      return b || stats.containsParam(paramFirstLowerCase(param));
   }

   public static String paramFirstLowerCase(String param) {
      return param.substring(0, 1).toLowerCase() + param.substring(1, param.length());
   }

   public static String paramFirstUpperCase(String param) {
      return param.substring(0, 1).toUpperCase() + param.substring(1, param.length());
   }

   @Override
   public String getStringParam(String param) {
      return stats.getStrParam(param);
   }

   public final String getPath() {
      return this.relevantPath;
   }
}
