import exception.ParameterNotFoundException;


import java.io.*;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 24/05/12
 */
public class StatisticsContainer {


   private Translation translation;
   private Array2DRowRealMatrix stats;
   private int numRows;
   private int numLines;



   public StatisticsContainer(String filePath) throws IOException{
     BufferedReader source = new BufferedReader(new FileReader(new File(filePath)));
     String header = source.readLine();
     this.translation = new Translation(header);


      this.numRows = translation.size();
      this.numLines = getNumLines(source);

      double[][] temp = new double[numLines][numRows];
      this.initializeStats(source,temp);
      stats = new Array2DRowRealMatrix(temp);
      source.close();

   }

   private void initializeStats(BufferedReader br,double[][] stats)throws IOException{
         String row;
      int i = 0;
         while((row = br.readLine())!=null){
            fillRow(stats, i, row);
         }
      }

   private void fillRow(double [][] stats, int i, String row){
      String[] split = row.split(";");
      int j = 0;
      for(String s : split){
         stats[i][j++] = Double.parseDouble(s);
      }
   }

   private int getNumLines(BufferedReader br)throws IOException{
      int i=0;
      while(br.readLine()!=null)
         i++;
      return i;
   }

   private String readAndReset(BufferedReader br) throws IOException{
      String ret = br.readLine();
      br.reset();
      return ret;
   }

   public double[] getParam(String param)throws ParameterNotFoundException{
    int index = this.translation.getParamIndex(param);
    return this.stats.getColumn(index);
   }

}
