
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
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


public class Index extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(Index.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Index(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job =new Job(new Configuration(), "Index");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(Text.class);	
   return job.waitForCompletion(true) ? 0 : 1;
  }





////mapper class beaking each word and converting it to word and offset as key value pair ,which  stored on local and later used by reducer

  public static class Map extends Mapper<LongWritable,Text, Text, Text> {
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
            System.out.println("current word="+currentWord+" filename="+fileNameValue);
            context.write(currentWord,fileNameValue);
        }
    }
  }
/////each pair of key is given to a reducer and reducer combines document ID for each word as postings and writes it to file .Each postin is sorted internally
  public static class Reduce extends Reducer<Text, Text, Text, Text> {
	  
	    @Override
	    public void reduce(Text word, Iterable<Text> values, Context context)

	        throws IOException, InterruptedException {
	    	StringBuilder sb = new StringBuilder();
	    	boolean flag=true;
	    	Text sumMulti = new Text("null") ;
	        ArrayList<String> al = new ArrayList<String>(); 
	      for (Text count : values) {
	          System.out.println("current word="+word+" filename="+count);

	        String[] parts = count.toString().split(".txt");
	        if(sumMulti.toString().contains(parts[0])) {
	        }else {
	        	al.add(parts[0]);
	       sumMulti=new Text(parts[0]);
	        }      	
	      }
	      Collections.sort(al); 
          sb.append(": "+al.get(0)+ ".txt");
	      for (int i=1; i<al.size(); i++) {
	          System.out.println(al.get(i));
	          sb.append(","+al.get(i)+ ".txt");
	          } 
	      sb.append(",");
	      Text sum=new Text(sb.toString());
	      context.write(word, sum);
	    }
	  }
  
}
