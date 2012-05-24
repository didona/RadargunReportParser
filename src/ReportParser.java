import java.io.IOException;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 24/05/12
 */
public abstract class ReportParser {


   private StatisticsContainer stats;

   public ReportParser(String path) throws IOException{
      stats = new StatisticsContainer(path);
   }

   public double getAvgParam(String param){

      double[] values = param(param);
      int i = 1;
      for(;i<values.length;i++){
         values[i]+=values[i-1];
      }
      return values[i]/(i+1);

   }

   protected final double[] getParam(String param){
      return param(param);

   }

   private double[] param(String param){
      return this.stats.getParam(param);
   }

}
