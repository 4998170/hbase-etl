package com.vung.hbaseprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;


public class HBaseJobCreation {

	public static 	String OUTPUT_COLUMN	=	"outputColumn";
	public static 	String VERBOSE			=	"verbose";
	public static 	String PROCESSOR_NAME	=	"processorName";
	public static String ARGUMENT_LIST		=	"arguments";
	public static String CACHED_FILES		=	"cachedfiles";

	public static String prefixName	=	"t2c";

	public static String ARGUMENT_SPLITTER	=	"@";

	public static final String MAX_WORDS 	= 	"maxwords";

	public static HBaseConfiguration  initConfig(String outputColumn, boolean isVerbose) {
		HBaseConfiguration config 	= 	new HBaseConfiguration();
		config.set(OUTPUT_COLUMN, outputColumn);
		if(isVerbose) config.set(VERBOSE, "true"); else config.set(VERBOSE, "false");

		config.setBoolean("mapred.map.tasks.speculative.execution", false);
		config.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		// experiment for more tolerate

		return config;
	}

	public static Job createHBaseJob(HBaseConfiguration config, String className, Class<? extends TableMapper> mapper, String jobName, String tableName, String targetTable, String inputColumn, String outputColumn, boolean isVerbose) {
		Job job	=	null;
		if(isVerbose) System.out.println("Setting job jar class:" + className);
		try {
			job = new Job(config, prefixName + "_" + jobName);
			try {
				job.setJarByClass(Class.forName(className));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				if(isVerbose) System.out.println("Exception setting job jar class:" + className);
			}  
			// class that contains mapper
			try {
				Scan scan 				= 	new Scan();
				//			scan.setCaching(500);        
				scan.setCacheBlocks(false);  
				

				//specific the column
				scan.addColumns(inputColumn);
				TableMapReduceUtil.initTableMapperJob(
						tableName,        // input HBase table name
						scan,             // Scan instance to control CF and attribute selection
						mapper,   // mapper
						null,             // mapper output key 
						null,             // mapper output value
						job);
				TableMapReduceUtil.initTableReducerJob(
						targetTable,      // output table
						null,             // reducer class
						job);

				job.setNumReduceTasks(0);
			}
			catch (NotServingRegionException e) {
				System.out.println("notserving region:" + e.getMessage());
				e.printStackTrace();
			}
			catch (RetriesExhaustedException e) {
				System.out.println("RetriesExhaustedException:" + e.getMessage());
				e.printStackTrace();
			}
			catch (NullPointerException e) {
				System.out.println("NullPointerException:" + e.getMessage());
				e.printStackTrace();
			}
			catch (UnknownScannerException e) {
				System.out.println("UnknownScannerException:" + e.getMessage());
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return job;
	}
	
	public static Job createHBaseJobFirstKeyFilter(HBaseConfiguration config, String className, Class<? extends TableMapper> mapper, String jobName, String tableName, String targetTable, String inputColumn, String outputColumn, boolean isVerbose) {
		Job job	=	null;
		if(isVerbose) System.out.println("Setting job jar class:" + className);
		try {
			job = new Job(config, prefixName + "_" + jobName);
			try {
				job.setJarByClass(Class.forName(className));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				if(isVerbose) System.out.println("Exception setting job jar class:" + className);
			}  
			// class that contains mapper
			try {
				Scan scan 				= 	new Scan();
				//			scan.setCaching(500);        
				scan.setCacheBlocks(false);  
				
				FirstKeyOnlyFilter filter	=	new FirstKeyOnlyFilter();
				scan.setFilter(filter);

				//specific the column
				scan.addColumns(inputColumn);
				TableMapReduceUtil.initTableMapperJob(
						tableName,        // input HBase table name
						scan,             // Scan instance to control CF and attribute selection
						mapper,   // mapper
						null,             // mapper output key 
						null,             // mapper output value
						job);
				TableMapReduceUtil.initTableReducerJob(
						targetTable,      // output table
						null,             // reducer class
						job);

				job.setNumReduceTasks(0);
			}
			catch (NotServingRegionException e) {
				System.out.println("notserving region:" + e.getMessage());
				e.printStackTrace();
			}
			catch (RetriesExhaustedException e) {
				System.out.println("RetriesExhaustedException:" + e.getMessage());
				e.printStackTrace();
			}
			catch (NullPointerException e) {
				System.out.println("NullPointerException:" + e.getMessage());
				e.printStackTrace();
			}
			catch (UnknownScannerException e) {
				System.out.println("UnknownScannerException:" + e.getMessage());
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return job;
	}
}
