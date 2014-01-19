package parse.radargun;


import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class Ispn5_2CsvParser extends RadargunCsvParser {

   public Ispn5_2CsvParser(String path) throws IOException {
      super(path);
   }

   public double numWarehouses() {
      if (isParam("NUM_WAREHOUSES"))
         return getAvgParam("NUM_WAREHOUSES");
      return 0;
   }

   public double writeThroughput() {
      double time = getTestSecDuration();
      double writes = numWriteXact();
      return writes / time;
   }

   public double readThroughput() {
      double time = getTestSecDuration();
      double reads = numReadXact();
      return reads / time;
   }

   public double usecThroughput() {
      return (writeThroughput() + readThroughput()) / 1e6D;
   }

   public double numThreads() {
      return getAvgParam("NUM_THREADS");
   }

   public double numKeys() {
      return getAvgParam("NUM_KEYS");
   }

   public double localReadProbability() {
      double remoteReads = getAvgParam("NumberOfRemoteGets");
      double totalReads = getAvgParam("NumberOfGets");
      return 1.0D - remoteReads / totalReads;
   }

   public double numRemoteGets() {
      return getSumParam("NumberOfRemoteGets");
   }

   public double remoteNodesInCommit() {
      return getAvgParam("AvgNumAsyncSentCommitMsgs");
   }

   public double nodesInCommit() {
      return getAvgParam("AvgNumNodesCommit");
   }

   public double prepareRtt() {
      return getAvgParam("AvgPrepareRtt");
   }

   public double holdTime() {
      return getAvgParam("AvgLockHoldTime");
   }

   public double localHoldTime() {
      return getAvgParam("AvgLocalLockHoldTime");
   }

   public double localSuccessfulHoldtime() {
      return getAvgParam("AvgLocalSuccessfulLockHoldTime");
   }

   public double localLocalAbortHoldTime() {
      return getAvgParam("AvgLocalLocalAbortLockHoldTime");
   }

   public double localRemoteAbortHoldTime() {
      return getAvgParam("AvgLocalRemoteAbortLockHoldTime");
   }

   public double remoteHoldTime() {
      return getAvgParam("AvgRemoteLockHoldTime");
   }

   public double netAsyncCommit() {
      return getAvgParam("AvgCommitAsync");
   }

   public double netAsyncRollback() {
      return getAvgParam("AvgRollbackAsync");
   }

   public double replicationDegree() {
      return getAvgParam("ReplicationDegree");
   }

   public double cpu() {
      return getAvgParam("CPU_USAGE");
   }

   public double mem() {
      return getAvgParam("MEM_USAGE");
   }


   //TODO: take the stats also at ispn level
   //OLD_FROM
  /*
   public double earlyAbortProbability() {
      double localFailures = getAvgParam("LOCAL_FAILURES");
      double failed = getAvgParam("LOCAL_FAILURES") + getAvgParam("REMOTE_FAILURES");
      double ok = getAvgParam("WRITE_COUNT");
      return localFailures / (failed + ok);
   }

   public double prepareLocalAbortProbability(){
      double localPrepareAb = numLocalPrepareAborts();
      double preparedXact = getAvgParam("WRITE_COUNT") + numRemotePrepareAborts() + localPrepareAb;
      return localPrepareAb / preparedXact;
   }

   public double prepareRemoteAbortProbability(){
      double remotePrepareAb = numRemotePrepareAborts();
      double remotePreparedXact = getAvgParam("WRITE_COUNT") + remotePrepareAb;
      return remotePrepareAb / remotePreparedXact;
   }


   public double numEarlyAborts() {
      return numAborts() - (numLocalPrepareAborts() + numRemotePrepareAborts());
   }

   public double numLocalPrepareAborts() {
      double prepareDead = numXactToPrepare() - numWriteXact();
      return prepareDead - numRemotePrepareAborts();
   }

   //OLD_TO
   */
   public double numRemotePrepareAborts() {
      return getSumParam("RemotelyDeadXact");
   }

   // NEW_FROM
   public double numEarlyAborts() {
      return getSumParam("NumEarlyAborts");
   }

   public double numLocalPrepareAborts() {
      return getSumParam("NumLocalPrepareAborts");
   }

   public double earlyAbortProbability() {
      double numEarlyAborts = numEarlyAborts();
      double localAborts = numLocalPrepareAborts();
      double remoteAborts = numRemotePrepareAborts();
      double ok = numWriteXact();
      double allWr = numEarlyAborts + localAborts + remoteAborts + ok;
      return numEarlyAborts / allWr;
   }

   public double prepareLocalAbortProbability() {
      double localAborts = numLocalPrepareAborts();
      double remoteAborts = numRemotePrepareAborts();
      double ok = numWriteXact();
      double allWr = localAborts + remoteAborts + ok;
      return localAborts / allWr;
   }

   public double prepareRemoteAbortProbability() {
      double remoteAborts = numRemotePrepareAborts();
      double ok = numWriteXact();
      double allWr = remoteAborts + ok;
      return remoteAborts / allWr;
   }
   //NEW_TO


   public String getReplicationProtocol() {
      if (isParam("currentProtocolId")) {
         return getStringParam("currentProtocolId");
      }
      int id = -1;
      if (isParam("currentProtocolAsInt")) {
         id = (int) this.getAvgParam("currentProtocolAsInt");
      } else if (isParam("CurrentProtocolAsInt")) {
         id = (int) this.getAvgParam("CurrentProtocolAsInt");
      }
      if (id != -1) {
         if (id == 0)
            return "2PC";
         if (id == 1)
            return "PB";
         return "TO";
      }
      if (super.relevantPath.contains("PB"))
         return "PB";
      if (super.relevantPath.contains("TO") || getAvgParam("tOGMUPrepareRttMinusMaxValidationTime") > 0) {
         return "TO";
      }
      return "2PC";
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

   public double remoteReadsPerROXact() {
      return getAvgParam("AvgRemoteGetsPerROTransaction");
   }

   public double remoteReadsPerWrXact() {
      return getAvgParam("AvgRemoteGetsPerWrTransaction");
   }

   public double writePercentageXact() {
      return getAvgParam("PercentageSuccessWriteTransactions");
   }

   public double readsPerROXact() {
      return getAvgParam("AvgGetsPerROTransaction");
   }

   public double readsPerWrXact() {
      return getAvgParam("AvgGetsPerWrTransaction");
   }

   public double putsPerWrXact() {
      return getAvgParam("AvgPutsPerWrTransaction");
   }

   public double remoteGetRtt() {
      return remoteGetRttNoWait();
   }

   public double remoteGetRttNoWait() {
      if (isParam("avgGmuClusteredGetCommandRttNoWait")) {
         if (getAvgParam("avgGmuClusteredGetCommandRttNoWait") != 0) {
            return getAvgParam("avgGmuClusteredGetCommandRttNoWait");
         }
      }
      return getAvgParam("avgRemoteGetRtt");
   }
   /*
   public double remoteGetRtt() {
      if (isParam("avgGmuClusteredGetCommandRttNoWait"))
         return getAvgParam("avgGmuClusteredGetCommandRttNoWait");
      return getAvgParam("AvgRemoteGetRtt");
   }

   public double remoteGetRttNoWait() {
      if (isParam("avgGmuClusteredGetCommandRttNoWait"))
         return getAvgParam("avgGmuClusteredGetCommandRttNoWait");
      log.warn("avgGmuClusteredGetCommandRttNoWait not available. Returning the time with remote waits");
      return remoteGetRtt();
   }
   */

   public double numReadsBeforeFirstWrite() {
      return getAvgParam("NumReadsBeforeWrite");
   }

   public double sizePrepareMsg() {
      return getAvgParam("AvgPrepareCommandSize");
   }

   public double sizeCommitMsg() {
      return getAvgParam("AvgCommitCommandSize");
   }

   public double sizeRollbackMsg() {
      return getAvgParam("AvgRollbackCommandSize");
   }

   public double sizeRemoteGetMsg() {
      return getAvgParam("AvgClusteredGetCommandSize");
   }

   public double sizeRemoteGetReplyMsg() {
      return getAvgParam("AvgClusteredGetCommandReplySize");
   }

   //TODO: switch to ISPN stats
   public double numRGAborts() {
      return getSumParam("LOCAL_FAILURES") + getSumParam("REMOTE_FAILURES");
   }

   public double RGSuxWrXactR() {
      return getAvgParam("SUX_UPDATE_XACT_RESPONSE");
   }

   public double RGROXactR() {
      return getAvgParam("SUX_READ_ONLY_XACT_RESPONSE");
   }

   public double RGWrCommitR() {
      return getAvgParam("COMMIT_TIME");
   }

   public double numAborts() {
      return getSumParam("NumAbortedXacts");
   }

   public double numWriteXact() {
      return getSumParam("WRITE_COUNT");
   }

   public double numReadXact() {
      return getSumParam("READ_COUNT");
   }


   public double numXactToPrepare() {
      return getSumParam("UpdateXactToPrepare");
   }

   public double remoteCommitWaitTime() {
      return getAvgParam("WaitedTimeInRemoteCommitQueue");
   }

   public double localCommitWaitTime() {
      return getAvgParam("WaitedTimeInLocalCommitQueue");
   }

   public double remoteGetWaitTime() {
      return getAvgParam("GMUClusteredGetCommandWaitingTime");
   }

   public double totalResponseTimeWrXact() {
      return getAvgParam("LocalUpdateTxTotalResponseTime");
   }

   public double totalResponseTimeROXact() {
      return getAvgParam("ReadOnlyTxTotalResponseTime");
   }

   public double businessLogicWrXactR() {
      double remoteGetCost = localRemoteGetResponseTime();
      double numRemoteRd = remoteReadsPerWrXact();
      double local = localResponseTimeWrXact();
      return local - remoteGetCost * numRemoteRd;
   }

   public double businessLogicROXactR() {
      double remoteGetCost = localRemoteGetResponseTime();
      double numRemoteRd = remoteReadsPerROXact();

      double local = localResponseTimeROXact();
      return local - remoteGetCost * numRemoteRd;
   }

   public double businessLogicWrXactS() {
      double remoteGetCost = localRemoteGetServiceTime();
      double numRemoteRd = remoteReadsPerWrXact();
      double local = localServiceTimeWrXact();
      return local - remoteGetCost * numRemoteRd;
   }

   public double businessLogicROXactS() {
      double remoteGetCost = localRemoteGetServiceTime();
      double numRemoteRd = remoteReadsPerROXact();
      double local = localServiceTimeROXact();
      return local - remoteGetCost * numRemoteRd;
   }


   public double localRemoteGetResponseTime() {
      return getAvgParam("RemoteGetResponseTime");
   }


   public double localRemoteGetServiceTime() {
      return getAvgParam("RemoteGetServiceTime");
   }


   public double localResponseTimeWrXact() {
      return getAvgParam("LocalUpdateTxLocalResponseTime");
   }

   public double localResponseTimeROXact() {
      return getAvgParam("LocalReadOnlyTxLocalResponseTime");
   }

   public double localServiceTimeWrXact() {
      return getAvgParam("LocalUpdateTxLocalServiceTime");
   }

   public double totalServiceTimeWrXact() {
      return getAvgParam("LocalUpdateTxTotalCpuTime");
   }

   public double localServiceTimeROXact() {
      return getAvgParam("LocalReadOnlyTxLocalServiceTime");
   }

   public double remoteRemoteGetServiceTime() {
      return getAvgParam("GMUClusteredGetCommandServiceTime");
   }

   public double remoteRemoteGetResponseTime() {
      return getAvgParam("GMUClusteredGetCommandResponseTime");
   }

   public double prepareCommandServiceTime() {
      return getAvgParam("LocalUpdateTxPrepareServiceTime");
   }

   public double prepareCommandResponseTime() {
      return getAvgParam("LocalUpdateTxPrepareResponseTime");
   }

   public double commitCommandServiceTime() {
      return getAvgParam("LocalUpdateTxCommitServiceTime");
   }

   public double commitCommandResponseTime() {
      return getAvgParam("LocalUpdateTxCommitResponseTime");
   }


   public double numLocalCommitsWait() {
      return getSumParam("NumWaitedLocalCommits");
   }

   public double numLocalUpdateCommits() {
      return getSumParam("WRITE_COUNT");
   }

   public double numRemoteCommitsWait() {
      return getAvgParam("NumWaitedRemoteCommits");
   }

   public double numRemoteGetsWait() {
      return getSumParam("NumWaitedRemoteGets");
   }


   public double remoteCommitCommandServiceTime() {
      return getAvgParam("RemoteUpdateTxCommitServiceTime");
   }

   public double remoteCommitCommandResponseTime() {
      return getAvgParam("RemoteUpdateTxCommitResponseTime");
   }


   public double localLocalRollbackServiceTime() {
      return getAvgParam("LocalUpdateTxLocalRollbackServiceTime");
   }

   public double localLocalRollbackResponseTime() {
      return getAvgParam("LocalUpdateTxLocalRollbackResponseTime");
   }

   public double localRemoteRollbackServiceTime() {
      return getAvgParam("LocalUpdateTxRemoteRollbackServiceTime");
   }

   public double localRemoteRollbackResponseTime() {
      return getAvgParam("LocalUpdateTxRemoteRollbackResponseTime");
   }

   public double remoteRollbackServiceTime() {
      return getAvgParam("RemoteUpdateTxRollbackServiceTime");
   }

   public double remoteRollbackResponseTime() {
      return getAvgParam("RemoteUpdateTxRollbackResponseTime");
   }

   public double remotePrepareServiceTime() {
      return getAvgParam("RemoteUpdateTxPrepareServiceTime");
   }

   public double remotePrepareResponseTime() {
      return getAvgParam("RemoteUpdateTxPrepareResponseTime");
   }


   public double unconditionalRemoteGetWaitTime() {
      return remoteGetWaitTime() * primaryOwnerRemoteGetWaitProbability();
   }

   public double unconditionalRemoteCommitWaitTime() {
      return asyncRemoteCommitWaitProbability() * remoteCommitWaitTime();
   }

   public double unconditionalLocalCommitWaitTime() {
      return localCommitWaitProbability() * localCommitWaitTime();
   }

   //NB: this only works if *only* the primary-owner is contacted
   public double primaryOwnerRemoteGetWaitProbability() {
      double allRemoteGets = getSumParam("NumberOfRemoteGets");
      if (allRemoteGets <= 0) {
         System.out.println("No remote gets. Read waiting time = 0");
         return 0;
      }
      double allWaitedRemoteGets = getSumParam("NumWaitedRemoteGets");
      return allWaitedRemoteGets / allRemoteGets;
   }

   public double localCommitWaitProbability() {
      double allLocalCommits = numWriteXact();
      double allWaitedLocalCommits = getSumParam("NumWaitedLocalCommits");
      return allWaitedLocalCommits / allLocalCommits;
   }

   public double asyncRemoteCommitWaitProbability() {
      double remoteNodesInCommit = getAvgParam("avgNumAsyncSentCommitMsgs");
      double allLocalCommits = numWriteXact();
      double allRemoteCommits = allLocalCommits * remoteNodesInCommit;
      double remoteWaitedCommits = getSumParam("numWaitedRemoteCommits");
      return remoteWaitedCommits / allRemoteCommits;
   }

   public double remoteAbortServiceTime() {
      return getAvgParam("remoteUpdateTxRollbackServiceTime");
   }


   /*
   PB
    */

   private int primaryIndex() {
      if (!getReplicationProtocol().equals("PB")) {
         System.out.println("Requesting primary index for NON PB");
      }
      double[] all = paramToArray("WRITE_COUNT");
      for (int i = 0; i < all.length; i++) {
         if (all[i] != 0)
            return i;
      }
      System.out.println("No primary found! Check your csv for WRITE_COUNT param");
      return -1;
   }

   public double primaryRemoteGets() {
      double[] all = getParam("NumberOfRemoteGets");
      return all[primaryIndex()];
   }

   public double backupRemoteGets() {
      return getSumParam("NumberOfRemoteGets") - primaryRemoteGets();
   }

   public double primaryRemoteGetRtt() {
      if (isParam("avgGmuClusteredGetCommandRttNoWait")) {
         double[] all = getParam("avgGmuClusteredGetCommandRttNoWait");
         return all[primaryIndex()];
      }
      log.warn("avgGmuClusteredGetCommandRttNoWait not available. Returning the time with remote waits");
      double[] all = getParam("AvgRemoteGetRtt");
      return all[primaryIndex()];
   }

   public double backupRemoteGetRtt() {
      double all = 0;
      if (isParam("avgGmuClusteredGetCommandRttNoWait")) {
         all = getSumParam("avgGmuClusteredGetCommandRttNoWait");
      } else {
         log.warn("avgGmuClusteredGetCommandRttNoWait not available. Returning the time with remote waits");
         all = getSumParam("AvgRemoteGetRtt");
      }
      return (all - primaryRemoteGetRtt()) / (getNumNodes() - 1D);
   }

   /**
    * TO
    */

   public double TOPrepareRtt() {
      return getAvgParam("avgTOGMUPrepareNoWaitRtt");
   }

   public double TOAvgConditionalWait() {
      return getAvgParam("tOAvgValidationConditionalWaitTime");
   }


}
