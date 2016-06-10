

import moa.classifiers.Classifier;

public class LearningModel implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int learner_num;
	public Classifier learner;
	
	public LearningModel(Classifier learner, int learner_num){
		this.learner = learner;
		this.learner_num = learner_num;
	}
}
