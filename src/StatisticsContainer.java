import exception.ParameterNotFoundException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 24/05/12
 */
public class StatisticsContainer {


   private Translation translation;
   private Array2DRowRealMatrix stats;
   private int numColumns;
   private int numRows;

   Log log = LogFactory.getLog(StatisticsContainer.class);

   public StatisticsContainer(String filePath) throws IOException {
      BufferedReader source = new BufferedReader(new FileReader(new File(filePath)));
      String header = source.readLine();
      this.translation = new Translation(header);


      this.numColumns = translation.size();
      this.numRows = getNumRows(filePath);

      double[][] temp = new double[numRows][numColumns];
      this.initializeStats(source, temp);
      stats = new Array2DRowRealMatrix(temp);
      source.close();

   }

   private void initializeStats(BufferedReader br, double[][] stats) throws IOException {
      String row;
      int i = 0;
      while ((row = br.readLine()) != null) {
         fillRow(stats, i, row);
         i++;
      }
   }

   private void fillRow(double[][] stats, int i, String row) {
      String[] split = row.split(",");
      int j = 0;
      for (String s : split) {
         try{
            stats[i][j] = Double.parseDouble(s);
         }
         catch(Exception e){
            log.warn("Trying to parse "+s+". Putting -1");
            stats[i][j] = -1;
         }
         j++;
      }
   }

   private int getNumRows(String f) throws IOException {
      int i = 0;
      BufferedReader br = new BufferedReader(new FileReader(new File(f)));
      while (br.readLine() != null)
         i++;
      return i - 1; //do not consider header!
   }


   public double[] getParam(String param) throws ParameterNotFoundException {
      int index = this.translation.getParamIndex(param);
      return this.stats.getColumn(index);
   }

   @SuppressWarnings("unused")
   private void dump(double[][] a, int row, int col) {
      for (int i = 0; i < row; i++) {
         for (int j = 0; j < col; j++)
            System.out.print(a[i][j] + ",");
         System.out.print("\n");
      }
   }


}
