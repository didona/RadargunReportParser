package parse.radargun.pb;

import exception.ParameterNotFoundException;
import parse.radargun.Ispn5_2CsvParser;

import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class Ispn5_2CsvParser_PB extends Ispn5_2CsvParser {
   protected final static String PB_DISCRIMINANT = "NumberOfPuts";
   private int pbIndex = pbIndex(PB_DISCRIMINANT);
   private final static PBHelper helper = new PBHelper();

   private double writePercentage = -1;

   public Ispn5_2CsvParser_PB(String path, double writePercentage) throws IOException {
      super(path);
      this.writePercentage = writePercentage;
   }

   /**
    * Instead of returning the whole column, I just return the column relevant to the primary
    *
    * @param param
    * @return
    */
   protected double[] paramToArray(String param) {
      try {
         if (helper.isPrimaryParam(param))
            return new double[]{this.stats.getParam(param)[pbIndex]};
         if (helper.isBackupParam(param))
            return backupValues(super.paramToArray(param), this.pbIndex);
         if (helper.isMultiParam(param))
            return super.paramToArray(param);
         throw new IllegalArgumentException(param + " is not marked as primary, backup or multi param!");
      } catch (ParameterNotFoundException p) {
         return this.stats.getParam(paramFirstLowerCase(param));
      }
   }

   private double[] backupValues(double[] all, int pbIndex) {
      double[] ret = new double[all.length - 1];
      for (int i = 0, j = 0; i < all.length; i++) {
         if (i != pbIndex) {
            ret[j] = all[i];
            j++;
         }
      }
      return ret;
   }

   private int pbIndex(String discriminant) {
      double[] values = super.paramToArray(discriminant);
      for (int i = 0; i < values.length; i++) {
         if (values[i] > 0)
            return i;
      }
      throw new IllegalArgumentException("Csv " + relevantPath + " is marked as PB but there is no slave with " + discriminant + " >0!!");
   }


   public double writeThroughput() {
      double time = getTestSecDuration();
      double writes = numWriteXact();
      return writes / time;
   }

   public double throughput() {
      double maxW = maxWriteThroughput();
      double maxR = maxReadThroughput();
      double wp = writePercentageXact();
      return Math.min(maxW / wp, maxR / (1 - wp));
   }

   public double readThroughput() {
      double time = getTestSecDuration();
      double reads = numReadXact();
      return reads / time;
   }

   public double maxWriteThroughput() {
      double time = getTestSecDuration();
      double writes = numWriteXact();
      return writes / time;
   }

   public double maxReadThroughput() {
      double time = getTestSecDuration();
      double reads = numReadXact();
      return reads / time;
   }

   public double writePercentageXact() {
      if (writePercentage != -1)
         return writePercentage;
      throw new IllegalStateException("Write Percentage for PB not defined!");
   }

   public double commitProbability() {
      double failed = getSumParam("LOCAL_FAILURES") + getSumParam("REMOTE_FAILURES");
      double ok = getSumParam("WRITE_COUNT") + getSumParam("READ_COUNT");
      return ok / (ok + failed);
   }

   public double writeXactCommitProbability() {
      double failed = getAvgParam("LOCAL_FAILURES") + getAvgParam("REMOTE_FAILURES");
      double ok = getAvgParam("WRITE_COUNT");
      return ok / (ok + failed);
   }
}
