package main.java.Entity;

import moa.classifiers.Classifier;

public class LearningModel implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int learner_num;
	public Classifier learner;
	public int counter;
	
	public LearningModel(Classifier learner, int learner_num, int counter){
		this.learner = learner;
		this.learner_num = learner_num;
		this.counter = counter;
	}
	
	public LearningModel(Classifier learner, int learner_num){
		this.learner = learner;
		this.learner_num = learner_num;
	}
}
