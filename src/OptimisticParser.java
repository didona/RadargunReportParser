import java.io.IOException;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 24/05/12
 */
public class OptimisticParser extends ParentParser{


   public OptimisticParser(String path) throws IOException{
      super(path);
   }


   public double getPrimaryParam(String param){
      double[] values = getParam(param);
      for(int i=0; i<values.length; i++){
         if(values[i]>0)
            return values[i];
      }
      return 0;
   }
}
