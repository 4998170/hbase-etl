package com.vung.processor;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class TextProcessor {
	public boolean isVerbose;
	public abstract String _process(String input);
	public abstract void init(Context cont, String args[], boolean isVerbose);
	
	public abstract void cleanup();
	
	
}