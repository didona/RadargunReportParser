package parse.geograph;

import exception.ParameterNotFoundException;
import parse.common.CsvParser;
import parse.timestamp.CsvTimestamp;

import java.io.IOException;

public class GeographCsvParser extends CsvParser implements RangeCsvParser_I {

   private CsvTimestamp init, end;

   public GeographCsvParser(String path) throws IOException {
      super(path);
   }

   public GeographCsvParser(String path, CsvTimestamp init, CsvTimestamp end) throws IOException {
      super(path);
      this.init = init;
      this.end = end;
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

   public double getAvgParam(String param, NODE_T node_t, CsvTimestamp from, CsvTimestamp to) {
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
      if (node_t.equals(NODE_T.MASTER))
         return values[i - 1] / (i) * getAvgParam(NUMBER_OF_NODES, from, to);
      else if (node_t.equals(NODE_T.SLAVE)) {
         double slaves = getAvgParam(NUMBER_OF_NODES);
         return values[i - 1] / (i) * (slaves - 1D) / slaves;
      } else
         return values[i - 1] / (i);
   }

   public double[] paramToArray(String param, CsvTimestamp from, CsvTimestamp to) {
      try {
         return this.stats.getParam(param, from, to);
      } catch (ParameterNotFoundException p) {
         return this.stats.getParam(paramFirstLowerCase(param), from, to);
      }
   }

   @Override
   public double getAvgParam(String param) {
      if (!isSetBoundaries())
         return super.getAvgParam(param);    //To change body of overridden methods use File | Settings | File Templates.
      return getAvgParam(param, init, end);
   }

   public double getAvgParam(String param, NODE_T node_t) {
      if (!isSetBoundaries())
         return super.getAvgParam(param);    //To change body of overridden methods use File | Settings | File Templates.
      return getAvgParam(param, node_t, init, end);
   }

   @Override
   public double[] getParam(String param) {
      if (!isSetBoundaries())
         return super.getParam(param);    //To change body of overridden methods use File | Settings | File Templates.
      return paramToArray(param, init, end);
   }

   @Override
   public String getStringParam(String param) {
      if (!isSetBoundaries())
         return super.getStringParam(param);    //To change body of overridden methods use File | Settings | File Templates.
      return getParam(param, init, end);
   }

   private boolean isSetBoundaries() {
      return init != null && end != null;
   }
}
