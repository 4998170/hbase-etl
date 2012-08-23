package com.vung.processor;

import java.util.HashMap;

import org.apache.hadoop.hbase.util.Bytes;


public abstract class HashMapProcessor extends TextProcessor {
	
	public abstract HashMap<byte[], byte[]> _processHashMap(String input);
	public HashMap<String, String> _processHashMapString(String input) {
		return null;
	}

}
