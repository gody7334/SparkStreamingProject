package moa.streams;

import moa.streams.ArffFileStream2;

public class ArffFileReader {

	public ArffFileStream2  ArffRead(String InputFile){
		String arffFilePath = InputFile;
		ArffFileStream2 arffStream = new ArffFileStream2(arffFilePath, -1);
		arffStream.prepareForUse();

		return arffStream;	
	}
	
}
