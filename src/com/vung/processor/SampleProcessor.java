package com.vung.processor;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class SampleProcessor extends TextProcessor{

	@Override
	public String _process(String input) {
		System.out.println("SampleProcessor default processor method");
		return null;
	}

	@Override
	public void init(Context cont, String[] args, boolean isVerbose) {
		try {
			DistributedCache.getLocalCacheFiles(cont.getConfiguration())[0].toString();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		System.out.println("SampleProcessor initilizing method:" + args);
	}
	
	public void cleanup() {
		
	}
	
	public SampleProcessor() {
		System.out.println("SampleProcessor default constructor");
	}

}
