package com.vung.hbaseprocessor;

import gnu.getopt.Getopt;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import com.vung.processor.HashMapProcessor;

public class HashMapProcessorJobs {
	
	private static 	String NAME				=	"Hashmap processor job ";
	private static 	String DEBUG			=	"debug";
	private static Logger logger 			= 	Logger.getLogger(HashMapProcessorJobs.class);
	

	public static  class Map extends TableMapper<ImmutableBytesWritable, Put>  {

		public String outputColumnFamily	=	"";
		public String outputColumnQualifier	=	"";
		public String processorClassName	=	"";
		
		public boolean isVerbose 			= 	false;
		public boolean debug				=	false;
		
		public HashMapProcessor hp;

		public void map(ImmutableBytesWritable row, Result value, Context context)  throws IOException, InterruptedException {
			Put put =	resultToPut(row,value);
			
			if (!debug && put.size() > 0) context.write(row, put);
		}
		

		/*
		 * input: wiki title
		 * output: normalized wiki title
		 */
		public Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
			Put put 						= new Put(key.get());
			String inputContent				=	"";
			for (KeyValue kv : result.raw()) {
				inputContent						=	Bytes.toString(kv.getValue());
				
				HashMap<byte[], byte[]> hm			=	hp._processHashMap(inputContent);
				
				//put
				//iterate
				Iterator<byte[]> it					=	(Iterator<byte[]>) hm.keySet().iterator();
				byte[] k							=	null;
				
				while (it.hasNext()) {
					k								=	it.next();
					
					//put key and value here
					if(outputColumnQualifier.equals("") || outputColumnQualifier == null) {
						put.add(Bytes.toBytes(outputColumnFamily), k, hm.get(k));
						if(isVerbose) System.out.println("HashMapProcessor outputColumnFamily:" + outputColumnFamily + " no column qualifier ");
					}
					else {
						put.add(Bytes.toBytes(outputColumnFamily), Bytes.toBytes(outputColumnQualifier), k);
						if(isVerbose) System.out.println("HashMapProcessor outputColumnFamily:" + outputColumnFamily + ":" +outputColumnQualifier + ":key:" +k);
					}
				}
				
			}
			return put;
		}

		public void setup(Context cont) {
			if(cont.getConfiguration().get(HBaseJobCreation.OUTPUT_COLUMN).contains(":")) {
				String[]	temp				= 	cont.getConfiguration().get(HBaseJobCreation.OUTPUT_COLUMN).split(":");
				outputColumnFamily				=	temp[0];					
				outputColumnQualifier			=	temp[1];
			}
			else outputColumnFamily				= 	cont.getConfiguration().get(HBaseJobCreation.OUTPUT_COLUMN);
			isVerbose							= 	(cont.getConfiguration().get(HBaseJobCreation.VERBOSE).equals("false")) ? false : true;
			processorClassName					=	cont.getConfiguration().get(HBaseJobCreation.PROCESSOR_NAME);
			String[] args						=  (cont.getConfiguration().get(HBaseJobCreation.ARGUMENT_LIST) == null) ? null : cont.getConfiguration().get(HBaseJobCreation.ARGUMENT_LIST).split(HBaseJobCreation.ARGUMENT_SPLITTER); 	
			
			debug								=	cont.getConfiguration().getBoolean(DEBUG, false);
			
			Class theClass;
			try {
				theClass 						= 	Class.forName(processorClassName);
				hp								=	(HashMapProcessor) theClass.newInstance();
				
				hp.init(cont, args, isVerbose);
				
			} catch (ClassNotFoundException e) {
				if(isVerbose) System.err.println("Class " + processorClassName + " not found");
				logger.error("Class " + processorClassName + " not found");
				e.printStackTrace();
			} catch (InstantiationException e) {
				if(isVerbose) System.err.println("Instantiation exception " + processorClassName);
				logger.error("Instantiation exception " + processorClassName);
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				if(isVerbose) System.err.println("IllegalAccessException exception " + processorClassName);
				logger.error("IllegalAccessException exception " + processorClassName);
				e.printStackTrace();
			}
		}

		
		public void cleanup(Context content) {
			//destroy tp
			try {
				System.out.println("mapper cleaning up");
				
				//cleanup distributed cached
				int i	=	DistributedCache.getLocalCacheFiles(content.getConfiguration()).length;
				for(int j=0; j<i; j++) {
					try {
						if(isVerbose) System.out.println("cleaning up distributed cache:" + j);
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

	public Job createJob(String processorClassName, String argumentList, String cachedFiles, String tableName, String targetTable, String inputColumn, String outputColumn, boolean debug, boolean isVerbose) throws IOException {

		
		HBaseConfiguration config 	= 	HBaseJobCreation.initConfig(outputColumn, isVerbose);
		
		config.set(HBaseJobCreation.PROCESSOR_NAME, processorClassName);
		config.setBoolean(DEBUG, debug);
		
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
		
		
		Job	job						=	HBaseJobCreation.createHBaseJob(config, this.getClass().getName(), Map.class, NAME, tableName, targetTable, inputColumn, outputColumn, isVerbose);
		return job;
	}

	public static void main(String argv[]) throws IOException, InterruptedException, ClassNotFoundException {
		
		String tableName				=	"";
		String inputColumn				=	"";
		String targetTableName			=	"";
		String outputColumn				=	"";
		
		String processorClassName		=	"";
		String arguments				=	"";
		String cachedFiles				=	"";
		
		
		boolean	isVerbose				=	false; 
		boolean debug					=	false;
		boolean isShowHelp 				= 	false;
		
		
		Getopt options 			= 	new Getopt("HashMapProcessorJobs", argv, "a:b:c:d:h:r:t:l:i:p:m:s:o:v:W:123");
		HashMapProcessorJobs	tj				=	new HashMapProcessorJobs();
		
		
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
			case 'r':
				targetTableName		=	options.getOptarg();
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
			default:
				System.err.println("unknown option");
			}

		}

		if(isShowHelp) {
			System.err.println("TextProcessingJobs:");
			System.err.println("    -t <string> : input table name");
			System.err.println("    -i <string> : input column family:qualifier");
			System.err.println("    -o <string> : input column family");
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
		
		//no target table, by default target table will be the same as table
		targetTableName						=	(targetTableName.equals("")) ? tableName : targetTableName;
		Job job 							= 	tj.createJob(processorClassName, arguments, cachedFiles, tableName, targetTableName, inputColumn, outputColumn, debug, isVerbose);	
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
