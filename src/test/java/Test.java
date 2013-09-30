import parse.geograph.GeographCsvParser;
import parse.timestamp.CsvTimestamp;

import java.io.IOException;

public class Test {

   public static void main(String[] args) {
      GeographCsvParser parser = null;
      try {
         parser = new GeographCsvParser("/Users/mcouceiro/Dropbox/cluster-1.csv");
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

      System.out.println(parser.getAvgParam("AvgNumNodesPrepare", new Ts(new Long("1375713250977")), new Ts(new Long("1375713712116"))));


   }
}

class Ts implements CsvTimestamp {

   private Long value;

   public Ts(Long v) {
      value = v;
   }

   @Override
   public Long toLong() {
      return value;
   }

}
