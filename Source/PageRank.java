/*
 Name: Rahul Reddy Arva
 ID: 800955965
 Title: PageRank
 Description: This is  MapReduce program. This program computes  the  PageRanks  of  an  input  set  of  hyperlinked  Wikipedia  documents  using  Hadoop  MapReduce.   
 The  PageRank  score  of  a  web  page serves as an indicator of the importance of the page.  Many web search engines (e.g., Google) use PageRank scores in some 
 form to rank user-submitted queries.
 
 */ 
package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank extends Configured implements Tool {
	   private static final Logger LOG = Logger .getLogger( PageRank.class);
	   public static void main(String[] args) throws Exception {   //main functions starts here
		   int res = ToolRunner.run(new PageRank(), args);
		   System.exit(res);
		 }
	   
	   public int run(String[] args) throws Exception {
		   
		   //Create job to get titles and links 
			Job job = Job.getInstance(getConf(), "PageRank");
			job.setJarByClass(this.getClass());
			//Input and output file paths are added here
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]+"_Intermediate1"));
			//Map and reduce functions are given jobs to execute
			job.setMapperClass(Mapper1.class);
			job.setReducerClass(Reducer1.class);
			//Output types are set to map and reduce functions
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.waitForCompletion(true); 
			  
			//Create configuration
			Configuration conf1 = getConf(); 
			Path path = new Path(args[1]+"_Intermediate1/part-r-00000");
			FileSystem fs = path.getFileSystem(conf1);
			int totalNodes = IOUtils.readLines(fs.open(path)).size(); //get the total number of nodes here
			conf1.setInt("totalNodes",totalNodes);
			Job job2;
			int i=01;
			 // Job is created to calculate page rank after performing 10 iterations 
			for(;i<10;i++){
			  	job2 = Job.getInstance(conf1, "PageRank"+i);
			    job2.setJarByClass(this.getClass());
			    //The generated path of job1 is set to job2
			    FileInputFormat.addInputPath(job2, new Path(args[1]+"_Intermediate"+i));
			    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_Intermediate"+(i+1)));
			    // Mapper2 and Reducer2 functions-jobs
			    
			    job2.setMapperClass(Mapper2.class);
			    job2.setReducerClass(Reducer2.class);
			    job2.setOutputKeyClass(Text.class);
			    job2.setOutputValueClass(Text.class);
			    job2.waitForCompletion(true);   
			} 
			
			Configuration cleanConf = getConf(); //create the configuration
			cleanConf.set("path", args[1]+"_Intermediate");
			Job job3 = Job.getInstance(cleanConf, "Clean"); //Creating the job to perform clean up and sorting
			job3.setJarByClass(this.getClass());
			FileInputFormat.addInputPath(job3, new Path(args[1]+"_Intermediate"+i)); // the path of job2 is set to job3
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			// Mapper3 and Reducer3 functions-jobs
			job3.setMapperClass(Mapper3.class);
			job3.setReducerClass(Reducer3.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			return job3.waitForCompletion(true) ? 0 : 1;
			    
		}

	   // Task-1 Create Link graph
	   // the aim of mapper1 function is to create link graph
	   
	   public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {  //Mapper1 starts here
		   private static final Pattern titlePattern = Pattern.compile(".*<title>(.*?)</title>.*");
		   private static final Pattern textPattern = Pattern.compile(".*<text.*?>(.*?)</text>.*");
		   private static final Pattern linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
		   		   
		   public void map(LongWritable offset, Text lineText, Context context)
		       throws IOException, InterruptedException {
			   	String line = lineText.toString();
			   	if(line.length()>0){
			   		StringBuffer stringBuffer1 = new StringBuffer("");
			       	Matcher title = titlePattern.matcher(line); 
			       	if(title.matches()){
			       		stringBuffer1.append(new Text(title.group(1).toString()));
			       	}
			       	
			       	Matcher textMatcher = textPattern.matcher(line); //Selecting the text with in the title tags
			       	StringBuffer stringBuffer2 = new StringBuffer("");
			       	if(textMatcher.matches())    //Selecting the text with in text tag
			       	{
			       		String links = textMatcher.group(1).toString();
			       		Matcher linkMatcher = linkPattern.matcher(links);
			       		//Selecting the links with in the list of links
			       		while(linkMatcher.find())
			       		{
			       			for(int j=0; j <linkMatcher.groupCount(); j++ )
			       			{
			       				stringBuffer2.append(linkMatcher.group(j+1).toString()+"####@@@@"); // a delimiter ####@@@@ is added between links
			       				context.write(new Text(linkMatcher.group(j+1).toString()),new Text(""));
			   					context.write(new Text(stringBuffer1.toString()),new Text(linkMatcher.group(j+1).toString()));
			   			    }			
			       	    }		
			       	}	
			       }
		   }
		 }
	   //Reducer1 is also used for creation of link graph 
	   public static class Reducer1 extends Reducer<Text, Text, Text, Text> {   //Reducer1 starts here.Output of Mapper1 is sent to reducer1.
		   @Override
		    public void reduce(Text key, Iterable<Text> links, Context context)
		    throws IOException, InterruptedException {
			   	int totalLinks = 0;
			    StringBuffer stringBuffer2 = new StringBuffer("");
			    for (Text link : links) {
			  	  if(link.toString().length()>0){      //if there is any link present, then this step is executed
			  		  stringBuffer2.append("####@@@@"+link.toString());  //delimiter is appended to the link string
			  		  totalLinks++;
			  	  }
			    } 
			    String value = "Rahul@@@@@"+ totalLinks + stringBuffer2.toString();    
			    Text text= new Text(value);
			    context.write(key, text);
		  }
		}
	   //Task-2 Processing the page rank
	   
	   public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> { // Mapper2 starts here. Output of reduce1 is sent to mapper2. The aim of mapper2 is to calculate pageRank
		   
		   public void map(LongWritable offset, Text lineText, Context context)
			       throws IOException, InterruptedException {
			   
			  	 try{
			  		 String line = lineText.toString();
			  		 
			  	     	if(line.length()>0){
			  	     		String[] split1 = line.split("\\t");  //split when encountered with a tab. SPlits as lines
			  	     		String[] split2 =split1[1].trim().split("@@@@@"); //split when encountered a delimiter
			  	     		
			  	     		if(split2.length>1){
			  	     			
			  	     			if(split2[0].equals("Rahul")){ //compare with Rahul
			      	     			int totalNodes =Integer.parseInt( context.getConfiguration().get("totalNodes"));
			      	     			context.write(new Text(split1[0]), new Text(((1/(double)totalNodes))+""));
			      	     		}
			      	     		else{
			      	     			double page_Rank = Double.parseDouble(split2[0].trim());
				      	     		String[] links = split2[1].split("####@@@@");
				      	     		int numLinks = Integer.parseInt(links[0].trim());
				      	     		if(numLinks>0){
				      	     			double pageR = (double)page_Rank/numLinks;
				      	     			for(int i = 1;i<links.length;i++){
				      	     				context.write(new Text(links[i]), new Text(pageR +""));
				      	     				}
				      	     			}
			      	     		
			      	     		}
			      	     		context.write(new Text(split1[0]), new Text(split1[1]));
			  	     		}	
			  	     	}
			  	 }catch(Exception e){
			  		 e.printStackTrace();
			  	 }

			   	
			   }

		 }
	   
	   //Reducer2 class to process the page rank
	   
	   public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		   
		   @Override
		   public void reduce(Text key, Iterable<Text> links, Context context)
		       throws IOException, InterruptedException {
			   
			   	double pageRank = 0;
			   	double dampingFactor = 0.85;
			   	StringBuffer stringBuffer1= new StringBuffer("");
			    StringBuffer stringBuffer2 = new StringBuffer("");
			      //    
			     for (Text link : links) {
			   	  if(link.toString().length()>0){
			   	     // Split the link using delimiter	
			   		  if(link.toString().contains("@@@@@")){
			   		      
			   			 String str[] = link.toString().split("@@@@@");
			   			 stringBuffer1.append(str[0]);
			   			 stringBuffer2.append(str[1]);
			   		  }
			   		  else{
			   			  pageRank = pageRank + Double.parseDouble(link.toString());
			   		  }

			   	  }
			     } 
			     pageRank = pageRank +(1-dampingFactor);
			     String value = pageRank+"@@@@@"+stringBuffer2.toString();
			     context.write(key, new Text(value));

		   }

		 } 


	   //Task- 3  Performing cleanup and sort
	   
	   //Mapper3 class to Perform cleanup and sort
	   public static class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
		   
		    public void map(LongWritable offset, Text lineText, Context context)
			        throws IOException, InterruptedException {
		    	
		    	  String line = lineText.toString();
		    	  if(line.length()>0){
			    		String[] split1 =  line.split("\\t");
			    		//Split the line using the delimiter "@@@@@"
			    		String[] split2 =  split1[1].trim().split("@@@@@");
			    		context.write(new Text("sorting"), new Text(split1[0]+"####@@@@"+split2[0]));

		    	  }

			    }

		  }
	   
	   
	   ////Reducer3 class to Perform cleanup and sort
	   public static class Reducer3 extends Reducer<Text, Text, Text, Text> {
		    @Override
		    public void reduce(Text key, Iterable<Text> links, Context context)
		        throws IOException, InterruptedException {
		    	
			      //Getting the path from getConfiguration function
			  	  String path= context.getConfiguration().get("path");
			  	  FileSystem fs = FileSystem.get(context.getConfiguration());
			  	  //Deleting the _Intermediate files
			  	  for(int i=1;i<=10;i++){
			  		  fs.delete(new Path(path+i),true);
			  	  }
			  	  //Sort the pages according to descending order in the rank 
			  	  ArrayList<Rank> page_Rank = new ArrayList<Rank>();
			  	
			    	for (Text link : links) {
			    		  if(link.toString().length()>0){
			    		  if(link.toString().contains("####@@@@")){
			    			 String str[] = link.toString().split("####@@@@");
			    			 
			    			 double rank = Double.parseDouble(str[1].trim());
			    			 Rank r = new Rank(str[0],rank);
			    			 page_Rank.add(r);
			    		  }
			    	  }  
			    	}
			    	Collections.sort(page_Rank, new Comparator<Rank>() {
	                    public int compare(Rank r1, Rank r2) {
	                        return Double.compare(r2.getRank(), r1.getRank());
	                    }
	                });
			    	for(int i=0; i< page_Rank.size();i++){
			    		Rank r1 = page_Rank.get(i);
			    		String rankText = r1.getRank()+"";
			    		context.write(new Text(r1.getTitle()), new Text(rankText));
			    	
			    	}
		    	

		    }


		   }

}
