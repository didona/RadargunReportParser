package parse.radargun.pb;

import parse.radargun.RgParserParams;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class PbParserParams extends RgParserParams {

   private double writePercentage;

   public PbParserParams(double writePercentage) {
      this.writePercentage = writePercentage;
   }

   public double getWritePercentage() {
      return writePercentage;
   }
}
