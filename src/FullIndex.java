
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import static java.util.Comparator.comparing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.*;

import org.apache.log4j.Logger;

public class FullIndex extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(Index.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new FullIndex(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	  
///// setting job ,input output directories 
    Job job =new Job(new Configuration(), "fullIndex");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    System.out.println("hashmap defx");

    job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(Text.class);
    
	
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
 
/////////////////////////////////////////////////
////************* full position index
////////////////////////////////////////
////mapper class beaking each word and converting it to word and (offset,docId) as key value pair ,which  stored on local and later used by reducer.Each posting is sorted internally
  public static class Map extends Mapper<LongWritable,Text,  Text,Text> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private long numRecords = 0;    
	    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	    public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
          String line = lineText.toString();
	      Text currentWord = new Text();
	      for (String word : WORD_BOUNDARY.split(line)) {
	        if (word.isEmpty()) {
	            continue;
	        }
	        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	        Text fileNameValue = new Text(fileName);
	         currentWord = new Text(word);
		    	StringBuilder sb = new StringBuilder();
		    	sb.append(fileNameValue+" | "+offset);
		        Text value = new Text(sb.toString());
	            context.write(currentWord,value);
	        }
	    }
	  }

/////each pair of key-value is given to a reducer 
//// reducer breakes value as documentId and offset and stored as object which are sorted internally
///each object checked for duplicate ,if duplicate postings for similar document are combined
// object along with its word is written to file
  public static class Reduce extends Reducer<Text, Text,Text, Text> {
 
    @Override
    public void reduce(Text word, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        ArrayList<PostingPojo> postingsList = new ArrayList<PostingPojo>(); 
    	StringBuilder post = new StringBuilder();

      for (Text posting : values) {
    	  PostingPojo postingpojo=new PostingPojo();
    	  String[] parts = posting.toString().split(".txt | ");
    	  postingpojo.setOffset(parts[2]);
    	  postingpojo.setFileName(parts[0]);
    	  postingsList.add(postingpojo);	  
          System.out.println("filename="+postingpojo.getFileName()+" pos="+postingpojo.getOffset());
      }
      Collections.sort(postingsList,new Comparator<PostingPojo>(){

		@Override
		public int compare(PostingPojo arg0, PostingPojo arg1) {
			// TODO Auto-generated method stub
			return arg0.getFileName().compareToIgnoreCase(arg1.getFileName());
		}
    	  
      });
      String copyFileName=null;
      String copyOffSet=null;
      post.append(": ");
      for (int i=0; i<postingsList.size(); i++) {
    	  post.append(postingsList.get(i).getFileName()+".txt@"+postingsList.get(i).getOffset());
    	  if(i+1<postingsList.size()) {
    		  post.append("+");  
    	  }
          } 
      post.append("+");
      Text sum=new Text(post.toString());
      context.write(word, sum);
    }
  }
  
  ////creating posting bean class so sorting and comparison is done easily and other computation
  public static class PostingPojo{
	  String offset;
	  String fileName;
	public String getOffset() {
		return offset;
	}
	public void setOffset(String offset) {
		this.offset = offset;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	  
  }
  }
  
 
  

