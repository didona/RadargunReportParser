import exception.ParameterNotFoundException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 24/05/12
 */
public abstract class ReportParser {


   private StatisticsContainer stats;
   Log log = LogFactory.getLog(ReportParser.class);

   public ReportParser(String path) throws IOException {
      stats = new StatisticsContainer(path);
   }

   public double getAvgParam(String param) {
      double[] values = null;
      try {
         values = param(param);
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
      double[] values = null;
      try {
         values = param(param);
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

   protected final double[] getParam(String param) {
      return param(param);

   }

   private double[] param(String param) {
      return this.stats.getParam(param);
   }

   public double getTestSecDuration() {
      return this.getAvgParam("DURATION(msec)") / 1000D;
   }

   public double getNumNodes() {
      return param("SLAVE_INDEX").length;
   }
}
