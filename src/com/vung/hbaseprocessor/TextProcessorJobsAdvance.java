package com.vung.hbaseprocessor;

import gnu.getopt.Getopt;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import com.vung.processor.TextProcessor;

public class TextProcessorJobsAdvance {

	private static 	String NAME				=	"Text processor job advance";
	private static 	String DEBUG			=	"debug";
	public static final String  BAD_ROWS	=	"org.wikipedia.en:http/wiki/User:";


	public static  class Map extends TableMapper<ImmutableBytesWritable, Put>  {

		public String outputColumnFamily	=	"";
		public String outputColumnQualifier	=	"";
		public String processorClassName	=	"";

		public boolean isVerbose 			= 	false;
		public boolean debug				=	false;


		public static TextProcessor  tp = null;

		public static TextProcessor  getInstance(String className) {
			if(tp == null) {
				if(tp == null) {
					Class theClass;
					try {
						theClass 						= 	Class.forName(className);
						tp								=	(TextProcessor) theClass.newInstance();

						System.out.println("getInstance: setting up:" + className);
					}
					catch (ClassNotFoundException e) {
						System.err.println("Class " + className + " not found");
						e.printStackTrace();
					} catch (InstantiationException e) {
						System.err.println("Instantiation exception " + className);
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						System.err.println("IllegalAccessException exception " + className);
						e.printStackTrace();
					}
					catch (Exception e) {
						System.err.println("exception initilizing instance:"  + e.getMessage());
						e.printStackTrace();
					}
				}
			}
			return tp;
		}


		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException, RetriesExhaustedException, NotServingRegionException  {
			Put put 						=	resultToPut(row,value); 

			try {
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
			catch (Exception e) {
				System.err.println("error while putting data to hbase:" + e.getMessage());
				e.printStackTrace();
			}
		}


		/*
		 * input: wiki title
		 * output: normalized wiki title
		 */
		public Put resultToPut(ImmutableBytesWritable key, Result result)  {
			Put put 						= 	new Put(key.get());
			String inputContent				=	"";
			String outputContent			=	"";

			try {
				for (KeyValue kv : result.raw()) {
					
					if(ignoredRow(Bytes.toString(key.get()))) continue;
					
					if (isVerbose) System.out.println("@@in:" + Bytes.toString(key.get()));
					
					inputContent						=	Bytes.toString(kv.getValue());
					outputContent						=	tp._process(inputContent);
					//					if (debug) System.out.println("@@out:" + Bytes.toString(key.get()));
					//put
					if(!debug)
						put.add(Bytes.toBytes(outputColumnFamily), Bytes.toBytes(outputColumnQualifier), Bytes.toBytes(outputContent));
				}
			}
			catch (Exception e) {
				System.err.println("error while generating data in mapper:" + e.getMessage());
				e.printStackTrace();
			}
			
			return put;
		}
		
		public boolean ignoredRow(String txtRow) {
			if(txtRow.contains(BAD_ROWS)) return true;
			else return false;
		}

		public void setup(Context cont) {

			String[]	temp				= 	cont.getConfiguration().get(HBaseJobCreation.OUTPUT_COLUMN).split(":");
			outputColumnFamily				=	temp[0];					
			outputColumnQualifier			=	temp[1];

			isVerbose						= 	(cont.getConfiguration().get(HBaseJobCreation.VERBOSE).equals("false")) ? false : true;
			debug							=	cont.getConfiguration().getBoolean(DEBUG, false);


			processorClassName				=	cont.getConfiguration().get(HBaseJobCreation.PROCESSOR_NAME);
			String[] args					=  	(cont.getConfiguration().get(HBaseJobCreation.ARGUMENT_LIST) == null) ? null : cont.getConfiguration().get(HBaseJobCreation.ARGUMENT_LIST).split(HBaseJobCreation.ARGUMENT_SPLITTER); 	

			if(isVerbose) System.out.println("start setting up mapper");
			
			Class theClass;
			try {
				//				theClass 						= 	Class.forName(processorClassName);
				//				tp								=	(TextProcessor) theClass.newInstance();
				tp							=	getInstance(processorClassName);
				
				
				tp.init(cont, args, isVerbose);
				if(isVerbose)  System.out.println("TextProcessorJobsAdvance init processor ");

			} 
			catch (Exception ex) {
				System.err.println("mapper setting up exception:" +  ex.getMessage());
				ex.printStackTrace();
			}

			if(isVerbose)  System.out.println("finish setup mapper");

		}

		public void cleanup(Context content) {
			//destroy tp
			try {
				System.out.println("mapper cleaning up");
				tp.cleanup();		
				tp	=	null;

				
				//cleanup distributed cached
				int i	=	DistributedCache.getLocalCacheFiles(content.getConfiguration()).length;
				for(int j=0; j<i; j++) {
					try {
						if(isVerbose) System.out.println("cleaning up distributed cache:" + j + ":" +  DistributedCache.getLocalCacheFiles(content.getConfiguration())[j].toString());
						DistributedCache.releaseCache(DistributedCache.getLocalCacheFiles(content.getConfiguration())[0].toUri(), content.getConfiguration());
						
					}
					catch (Exception ex) {
						//release cache failed
						System.err.println("releasing the cache file: failed:" + DistributedCache.getLocalCacheFiles(content.getConfiguration())[j].toUri());
					}
				}
				
				System.out.println("mapper finish cleaning up");
			}
			catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}

	public Job createJob(String processorClassName, String argumentList, String cachedFiles, String tableName, String targetTable, String inputColumn, String outputColumn, long mapperSkipRecords, boolean firstKeyFilter, boolean maxTolerate, boolean debug, boolean isVerbose) throws IOException {


		HBaseConfiguration config 	= 	HBaseJobCreation.initConfig(outputColumn, isVerbose);

		config.set(HBaseJobCreation.PROCESSOR_NAME, processorClassName);
		config.setBoolean(DEBUG, debug);

		if(maxTolerate) {
			config.setLong("mapred.skip.map.max.skip.records", Long.MAX_VALUE);
			config.setInt("mapred.max.tracker.failures", 300);
			config.setInt("mapred.tasktracker.map.tasks.maximum", 2);
		}
		if(!maxTolerate && mapperSkipRecords > 0) config.setLong("mapred.skip.map.max.skip.records", mapperSkipRecords);

		
		if(debug) config.setBoolean("keep.failed.task.files", true);
		//test to increase heap size
		//		config.set("mapred.child.java.opts","-Xmx3072m");

		if(!argumentList.equals("")) config.set(HBaseJobCreation.ARGUMENT_LIST, argumentList);
		if(!cachedFiles.equals("")) config.set(HBaseJobCreation.CACHED_FILES, cachedFiles);


		//add file to cached
		String[] arrCachedFiles			=		(cachedFiles.equals("")) ? null : cachedFiles.split(HBaseJobCreation.ARGUMENT_SPLITTER);
		if(arrCachedFiles != null) {
			for(int i=0; i < arrCachedFiles.length; i++) {
				try {
					DistributedCache.addCacheFile(new URI(arrCachedFiles[i]), config);
				} catch (URISyntaxException e) {
					e.printStackTrace();
				}
			}
		}

		Job job;
		if(!firstKeyFilter)
			job						=	HBaseJobCreation.createHBaseJob(config, this.getClass().getName(), Map.class, NAME, tableName, targetTable, inputColumn, outputColumn, isVerbose);
		else
			job						=	HBaseJobCreation.createHBaseJobFirstKeyFilter(config, this.getClass().getName(), Map.class, NAME, tableName, targetTable, inputColumn, outputColumn, isVerbose);
		return job;
	}

	public static void main(String argv[]) throws IOException, InterruptedException, ClassNotFoundException {

		String tableName				=	"";
		String inputColumn				=	"";
		String outputColumn				=	"";

		String processorClassName		=	"";
		String arguments				=	"";
		String cachedFiles				=	"";


		boolean	isVerbose				=	false; 
		boolean isShowHelp 				= 	false;
		boolean debug					=	false;
		boolean maxTolerate				=	false;
		boolean firstKeyFilter			=	false;

		long mapperSkipRecords				=	(long) 0;


		Getopt options 			= 	new Getopt("TextProcessingJobsAdvance", argv, "a:b:c:d:e:f:h:r:t:l:i:p:m:s:o:v:W:123");
		TextProcessorJobsAdvance	tj				=	new TextProcessorJobsAdvance();


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
			case 'p':
				processorClassName	=	options.getOptarg();
				break;
			case 'o':
				outputColumn		=	options.getOptarg();
				break;	
			case 'c':
				cachedFiles			=	options.getOptarg();
				break;	
			case 'a':
				arguments			=	options.getOptarg();
				break;	
			case 'e':
				maxTolerate			=	true;
				break;	
			case 'f':
				mapperSkipRecords	=	Long.parseLong(options.getOptarg());
				break;
			case 'r':
				firstKeyFilter		=	true;
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
			System.err.println("    -a <string[]> : arguments list, separated by @");
		}

		if(isVerbose) {
			System.out.println("tableName:" + tableName);
			System.out.println("inputColumn:" + inputColumn);
			System.out.println("outputColumn:" + outputColumn);
			System.out.println("processor class:" + processorClassName);
			System.out.println("arguments:" + arguments);
			System.out.println("cached files:" + cachedFiles);
		}

		Job job 							= 	tj.createJob(processorClassName, arguments, cachedFiles, tableName, tableName, inputColumn, outputColumn, mapperSkipRecords, firstKeyFilter, maxTolerate, debug, isVerbose);	
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
