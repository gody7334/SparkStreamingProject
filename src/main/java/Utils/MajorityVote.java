package main.java.Utils;

import java.util.List;

import main.java.Entity.PredictResult;

import weka.core.Utils;

public class MajorityVote implements java.io.Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static int Vote( PredictResult[] list_predict_results, int numOfClass){
		int numClass = numOfClass;
		int[] classVote = new int[numClass];
//		System.out.println("number of predict result: " + list_predict_results.length);
		for(PredictResult pr : list_predict_results){
			int classIdx = maxIndex(pr.result);
			classVote[classIdx]++;
		}
		
		int[] array = classVote;
		int largest = array[0];
		int index = 0;
		for (int i = 1; i < array.length; i++) {
		  if ( array[i] > largest ) {
		      largest = array[i];
		      index = i;
		   }
		}
		
		return index;
	}
	
	  public static/* @pure@ */int maxIndex(double[] doubles) {

	    double maximum = 0;
	    int maxIndex = 0;

	    for (int i = 0; i < doubles.length; i++) {
	      if ((i == 0) || (doubles[i] > maximum)) {
	        maxIndex = i;
	        maximum = doubles[i];
	      }
	    }

	    return maxIndex;
	  }
}
