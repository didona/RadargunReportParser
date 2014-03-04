package parse.radargun;

import parse.common.CsvParser;

import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class RadargunCsvParser extends CsvParser {

   private static final String SLAVE_PARAM = "SLAVE_INDEX";
   private static final String SEC_PARAM = "DURATION";
   private static final String SEC_PARAM_2 = "DURATION (msec)";

   public RadargunCsvParser(String path) throws IOException {
      super(path);
   }

   public double getTestSecDuration() {
      String p;
      if (isParam(SEC_PARAM))
         p = SEC_PARAM;
      else
         p = SEC_PARAM_2;
      return this.getAvgParam(p) / 1000D;
   }

   public double getNumNodes() {
      return paramToArray(SLAVE_PARAM).length;
   }
}

