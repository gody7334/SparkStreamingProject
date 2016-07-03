package main.java.Entity;

public class PredictResult implements java.io.Serializable{
	public int learner_num;
	public double[] result;
	
	public PredictResult( int learner_num, double[] result){
		this.learner_num = learner_num;
		this.result = result;
	}
	
}
