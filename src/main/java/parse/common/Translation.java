package parse.common;

import exception.ParameterNotFoundException;

import java.io.IOException;
import java.util.HashMap;

/**
 * Author: Diego Didona Email: didona@gsd.inesc-id.pt Websiste: www.cloudtm.eu Date: 24/05/12
 */
public class Translation {

   private HashMap<String, Integer> translation;

   public Translation(String header) throws IOException {
      this.translation = new HashMap<String, Integer>();
      this.init(header, this.translation);
   }

   public int getParamIndex(String param) throws ParameterNotFoundException {
      Integer i = this.translation.get(param);
      if (i == null)
         throw new ParameterNotFoundException(param);
      return i;
   }

   private void init(String header, HashMap<String, Integer> translation) {
      String[] read = header.split(",");
      int i = 0;
      for (String s : read) {
         this.translation.put(s, i++);
      }
   }

   public int size() {
      return this.translation.size();
   }

   public boolean exist(String s) {
      return translation.containsKey(s);
   }


}
