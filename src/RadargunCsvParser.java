import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class RadargunCsvParser extends CsvParser{

   private static final String SLAVE_PARAM = "SLAVES_INDEX";
   private static final String SEC_PARAM = "DURATION(msec)";

   public RadargunCsvParser(String path) throws IOException {
      super(path);
   }

   public double getTestSecDuration() {
      return this.getAvgParam(SEC_PARAM) / 1000D;
   }

   public double getNumNodes() {
      return paramToArray(SLAVE_PARAM).length;
   }
}

