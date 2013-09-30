package parse.geograph;

import exception.ParameterNotFoundException;
import parse.common.CsvParser;
import parse.timestamp.CsvTimestamp;

import java.io.IOException;

public class GeographCsvParser extends CsvParser implements RangeCsvParser_I {

   public GeographCsvParser(String path) throws IOException {
      super(path);
   }

   public String getParam(String param, CsvTimestamp from, CsvTimestamp to) {
      try {
         return stats.getStrParam(param, from, to);
      } catch (ParameterNotFoundException p) {
         return stats.getStrParam(param, from, to);
      }
   }

   public double getAvgParam(String param, CsvTimestamp from, CsvTimestamp to) {
      double[] values;
      try {
         values = paramToArray(param, from, to);
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

   public double getAvgParam(String param, boolean master, CsvTimestamp from, CsvTimestamp to) {
      double[] values;
      try {
         values = paramToArray(param, from, to);
      } catch (ParameterNotFoundException p) {
         log.warn("Parameter " + p + " not found. Returning -1");
         return -1;
      }
      int i = 1;
      for (; i < values.length; i++) {
         values[i] += values[i - 1];
      }
      if (master)
         return values[i - 1] / (i) * getAvgParam(NUMBER_OF_NODES, from, to);
      else
         return values[i - 1] / (i);
   }

   public double[] paramToArray(String param, CsvTimestamp from, CsvTimestamp to) {
      try {
         return this.stats.getParam(param, from, to);
      } catch (ParameterNotFoundException p) {
         return this.stats.getParam(paramFirstLowerCase(param), from, to);
      }
   }


}
