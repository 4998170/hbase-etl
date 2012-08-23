package com.vung.hbaseprocessor;

import gnu.getopt.Getopt;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import com.vung.processor.TextProcessor;

//import com.scanscout.coretech.normalized.*;


public class TextProcessingJobs {



	public static 	String PROCESSOR		=	"processor";
	public static 	String METHOD			=	"method";
	public static 	String DEBUG			=	"debug";


	public static 	TextProcessor tp;
	private static 	String NAME				=	"t2c_Text processing job";

	private static Logger logger 						= 	Logger.getLogger(TextProcessingJobs.class);


	public static  class Map extends TableMapper<ImmutableBytesWritable, Put>  {

		public String outputColumnFamily	=	"";
		public String outputColumnQualifier	=	"";
		public String processorClassName	=	"";
		public String methodName			=	"";
		public boolean isVerbose 			= 	false;
		public boolean debug				=	false;

		public void map(ImmutableBytesWritable row, Result value, Context context)  throws IOException, InterruptedException, RetriesExhaustedException, NotServingRegionException {
			try {
				Put put 						=	resultToPut(row,value); 
				if(!debug) 
					context.write(row, put);
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
		}

		public String process2(TextProcessor tp, String inputContent) {
			return tp._process(inputContent);
		}

		public String process(String processorClassName, String methodName, String inputContent) {
			String outputContent			=	"";

			logger.info("invoking " + processorClassName + "." + methodName);
			if(isVerbose) System.out.println("invoking " + processorClassName + "." + methodName);


			Method method					=	null;
			Object obj						=	null;
			try {

				method = Class.forName(processorClassName).getMethod(methodName, String.class);
			} catch (SecurityException e) {
				if(isVerbose) System.out.println("SecurityException:" + e.getMessage());
				logger.error("SecurityException:" + e.getMessage());
				e.getStackTrace();
			} catch (NoSuchMethodException e) {
				if(isVerbose) System.out.println("NoSuchMethodException:" + e.getMessage() + ":" + methodName);
				logger.error("NoSuchMethodException:" + e.getMessage() + ":" + methodName);
				e.getStackTrace();
			}
			catch (ClassNotFoundException e) {
				if(isVerbose) System.out.println("ClassNotFoundException:" + e.getMessage() +":" + processorClassName);
				logger.error("ClassNotFoundException:" + e.getMessage() +":" + processorClassName);
				e.getStackTrace();
			}
			try {
				outputContent	=	(String) method.invoke(obj, inputContent);
			} catch (IllegalArgumentException e) {
				if(isVerbose) System.out.println("IllegalArgumentException:" + e.getMessage());
				logger.error("IllegalArgumentException:" + e.getMessage());
				e.getStackTrace();
			} catch (IllegalAccessException e) {
				if(isVerbose) System.out.println("IllegalAccessException:" + e.getMessage());
				logger.error("IllegalAccessException:" + e.getMessage());
				e.getStackTrace();
			} catch (InvocationTargetException e) {
				if(isVerbose) System.out.println("InvocationTargetException:" + e.getMessage());
				logger.error("InvocationTargetException:" + e.getMessage());
				e.getStackTrace();
			}
			catch (Exception e) {
				e.printStackTrace();
				if(isVerbose) System.out.println("Exception invoking class:" + processorClassName + "." + methodName  + ":" + e.getMessage());
				logger.error("Exception invoking class:" + processorClassName + "." + methodName  + ":" + e.getMessage());
				e.getStackTrace();
			}
			return outputContent;
		}

		/*
		 * input: wiki title
		 * output: normalized wiki title
		 */
		public Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
			Put put 						= new Put(key.get());
			String inputContent				=	"";
			String outputContent			=	"";

			for (KeyValue kv : result.raw()) {
				inputContent						=	Bytes.toString(kv.getValue());

				outputContent						=	process(processorClassName, methodName, inputContent);
				//				outputContent						=	process2(tp, inputContent);

				if(isVerbose) System.out.println("output:" + outputContent);
				put.add(Bytes.toBytes(outputColumnFamily), Bytes.toBytes(outputColumnQualifier), Bytes.toBytes(outputContent));
			}
			return put;
		}

		public void setup(Context cont) {

			String[]	temp				= 	cont.getConfiguration().get(HBaseJobCreation.OUTPUT_COLUMN).split(":");
			outputColumnFamily				=	temp[0];					
			outputColumnQualifier			=	temp[1];

			processorClassName				= 	cont.getConfiguration().get(PROCESSOR);
			methodName						= 	cont.getConfiguration().get(METHOD);
			isVerbose						= 	(cont.getConfiguration().get(HBaseJobCreation.VERBOSE).equals("false")) ? false : true;

			debug							=	cont.getConfiguration().getBoolean(DEBUG, false);

			if(isVerbose) System.out.println("setting up " + processorClassName + "." + methodName);
		}

	}

	public Job createJob(String processorClassName, String methodName, String tableName, String targetTable, String inputColumn, String outputColumn, boolean debug, boolean isVerbose) throws IOException {


		HBaseConfiguration config 	= 	HBaseJobCreation.initConfig(outputColumn, isVerbose);
		config.set(PROCESSOR, processorClassName);
		config.set(METHOD, methodName);
		config.setBoolean(DEBUG, debug);


		Job	job						=	HBaseJobCreation.createHBaseJob(config, this.getClass().getName(), Map.class, NAME, tableName, targetTable, inputColumn, outputColumn, isVerbose);
		return job;
	}



	public static void main(String argv[]) throws IOException, InterruptedException, ClassNotFoundException {

		//		String tableName		=	"corpus_wikipedia";
		//		String inputColumn		=	"mtdt:tm_t";
		//		String outputColumn		=	"mtdt:raw_compound2";

		String processorClassName		=	"";//"com.scanscout.coretech.normalized.NormalizeWikiTitle";
		String methodName				=	"";//"_process";


		String tableName		=	"";
		String inputColumn		=	"";
		String outputColumn		=	"";
		boolean	isVerbose		=	false; 
		boolean isShowHelp 		= 	false;
		boolean debug			=	false;


		Getopt options 			= 	new Getopt("TextProcessingJobs", argv, "a:b:c:d:h:r:t:l:i:p:m:s:o:v:W:123");
		TextProcessingJobs	tj				=	new TextProcessingJobs();


		int c;
		while ((c = options.getopt()) != -1) {
			switch (c) {
			case 'h':
				isShowHelp			=	true;
				break;
			case 'v':
				isVerbose			=	true;
				break;
			case 'd':
				debug				=	true;
				break;
			case 't':
				tableName			=	options.getOptarg();
				break;
			case 'i':
				inputColumn			=	options.getOptarg();
				break;
			case 'o':
				outputColumn		=	options.getOptarg();
				break;	
			case 'p':
				processorClassName		=	options.getOptarg();
				break;	
			case 'm':
				methodName				=	options.getOptarg();
				break;	
			default:
				System.err.println("unknown option");
			}

		}

		if(isShowHelp) {
			System.err.println("TextProcessingJobs:");
			System.err.println("    -t <string> : input table name");
			System.err.println("    -i <string> : input column family:qualifier");
			System.err.println("    -o <string> : input column family:qualifier");
			System.err.println("    -p <string> : input processor class name");
			System.err.println("    -m <string> : input method name");
		}

		if(isVerbose) {
			System.out.println("tableName:" + tableName);
			System.out.println("inputColumn:" + inputColumn);
			System.out.println("outputColumn:" + outputColumn);
			System.out.println("processor:" + processorClassName);
			System.out.println("methodName:" + methodName);
		}

		Job job 							= 	tj.createJob(processorClassName, methodName, tableName, tableName, inputColumn, outputColumn, debug, isVerbose);	
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
