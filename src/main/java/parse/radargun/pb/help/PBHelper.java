package parse.radargun.pb.help;

import parse.common.CsvParser;

import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class PBHelper {
   private final static Set<String> primaryParams = PrimaryParams.params;
   private final static Set<String> backupParams = BackupParams.params;
   private final static Set<String> multiParams = MultiParams.params;

   private boolean isParam(String s, Set<String> ss) {
      return ss.contains(s) || ss.contains(CsvParser.paramFirstLowerCase(s)) || ss.contains(CsvParser.paramFirstUpperCase(s));
   }

   public boolean isPrimaryParam(String s) {
      return isParam(s, primaryParams);
   }

   public boolean isBackupParam(String s) {
      return isParam(s, backupParams);
   }

   public boolean isMultiParam(String s) {
      return isParam(s, multiParams);
   }
}
