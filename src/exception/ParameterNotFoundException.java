package exception;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 24/05/12
 */
public class ParameterNotFoundException extends RuntimeException {
   public ParameterNotFoundException(String s) {
      super(s);
   }
}
