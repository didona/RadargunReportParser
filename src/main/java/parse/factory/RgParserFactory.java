package parse.factory;

import parse.radargun.Ispn5_2CsvParser;
import parse.radargun.RgParserParams;
import parse.radargun.pb.Ispn5_2CsvParser_PB;
import parse.radargun.pb.PbParserParams;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class RgParserFactory {

   public Ispn5_2CsvParser buildParser(String path,RgParserParams params) throws Exception {
      Ispn5_2CsvParser p = new Ispn5_2CsvParser(path);
      if (p.getReplicationProtocol().equals("PB"))
         return new Ispn5_2CsvParser_PB(path, (PbParserParams)params);
      return p;
   }
}
