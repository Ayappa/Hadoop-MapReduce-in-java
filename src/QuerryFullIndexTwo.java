
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import static java.util.Comparator.comparing;

import java.util.*;

import org.apache.log4j.Logger;


public class QuerryFullIndexTwo extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Index.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new QuerryFullIndexTwo(), args);
		System.exit(res);
	}

///// setting job ,input output directories 
	public int run(String[] args) throws Exception {
		Job job = new Job(new Configuration(), "wordcount");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		System.out.println("hashmap defx");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int flag = job.waitForCompletion(true) ? 0 : 1;
		//// if job is complete we process the querry for finding the matching document
		if (flag == 1) {
			/// creating arraylist of objects for query mapReduce output
			ArrayList<QuerryFullPojo> index = querryProcessingPosting(new Path(args[1]));
			/////if u dont execute please run this one
			Path path = new Path(new Path(args[2]) + "/part-r-00000");
			/////after running combine_file.py execute this line
			//Path path = new Path(new Path(args[2]) + "/combined_index");

			System.out.println("path===" + path);
			/// creating arraylist of objects for document mapReduce output
			ArrayList<QuerryFullPojo> querryList = querryProcessingPosting(path);
			compareQuerry(index, new Path(args[1]), querryList, path);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	///// comparing created querry arraylist and document array list for matches and
	///// writing it on to file

	private void compareQuerry(ArrayList<QuerryFullPojo> index, Path path, ArrayList<QuerryFullPojo> querryList,
			Path Qpath) throws IOException {
		// TODO Auto-generated method stub
		PrintWriter writer = null;
		FileWriter filewriter = null;

		try {
			writer = new PrintWriter(Qpath.toString());
			writer.print("");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			filewriter = new FileWriter(Qpath.toString(), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		///// looping through query arraylist
		for (int i = 0; i < querryList.size(); i++) {
			StringBuilder sb = new StringBuilder();
			sb.append(querryList.get(i).getWord() + " : ");
			sb.append(System.getProperty("line.separator"));
			///// looping through document arraylist
			for (int j = 0; j < index.size(); j++) {
				//// if querry word and document matches then print all its offset lines of that
				//// particular documnet
				if (querryList.get(i).getWord().equals(index.get(j).getWord())) {
					boolean flag = true;
					///// looping through document arraylists-filename arrayList
					for (int m = 0; m < index.get(j).getFileName().size(); m++) {
						if (!flag) {
							sb.append(System.getProperty("line.separator"));
							flag = true;
						}
						;
						String hi = readFromRandomAccessFile("Input/" + index.get(j).getFileName().get(m),
								Integer.parseInt(index.get(j).getOffset().get(m)));
						sb.append(
								index.get(j).getFileName().get(m) + "@" + index.get(j).getOffset().get(m) + "->" + hi);
						sb.append(System.getProperty("line.separator"));
						// flag = false;
					}
					// sb.append(System.getProperty("line.separator"));

					try {
						filewriter.write(sb.toString());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(sb.toString());
					sb.setLength(0);

				}
			}
		}
		filewriter.close();
	}

/// reading a a file from a path and at a given offset
	public static String readFromRandomAccessFile(String file, int position) {
		String record = null;
		try {
			RandomAccessFile fileStore = new RandomAccessFile(file, "rw");
			// moves file pointer to position specified
			fileStore.seek(position);
			// reading String from
			record = fileStore.readLine();
			fileStore.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return record;
	}

//////////////////converting map reduce job to arraylist for comparing querry and Documents
	private ArrayList querryProcessingPosting(Path path) throws IOException {
		// TODO Auto-generated method stub
		String name = path.toString();
		BufferedReader br = null;
		ArrayList<QuerryFullPojo> querryList = new ArrayList<QuerryFullPojo>();
		try {
			br = new BufferedReader(new FileReader(name));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			String[] parts = null;
			String[] partres = null;
			while (line != null) {
				ArrayList<String> offset = new ArrayList<String>();
				ArrayList<String> fileName = new ArrayList<String>();
				QuerryFullPojo querryfullpojo = new QuerryFullPojo();
				System.out.println("statwment----------------------=@" + line);
				parts = line.split("[: ]");
				querryfullpojo.setWord(parts[0]+" "+parts[1]);
				if(parts[0].isEmpty()) {
					line = br.readLine();
					continue;
				}
				System.out.println("word======" + parts[0]);
				//int m=2;
				if(parts[1].isEmpty()) {
					partres = parts[2].split("[+]");
				}else
				if(parts[2].isEmpty()) {
					partres = parts[3].split("[+]");
				}else {
				partres = parts[4].split("[+]");
				}
				
				
				for (int i = 0; i < partres.length; i++) {
					System.out.println("length======" + partres[i]);
					String[] partresult = partres[i].split("@");
					fileName.add(partresult[0]);
					offset.add(partresult[1]);

				}
				querryfullpojo.setFileName(fileName);
				querryfullpojo.setOffset(offset);
//				String[] partresult =  partres[0].split("@");
//				System.out.println("length======" + partresult[0]);
//				System.out.println("length======" + partresult[1]);
				System.out.println("word=======" + querryfullpojo.getWord() + "file==" + querryfullpojo.getFileName()
						+ " post==" + querryfullpojo.getOffset());
				querryList.add(querryfullpojo);
				line = br.readLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
		return querryList;
	}

//////////creating bean class for storing postings which contains arraylist of offset and filename
	public static class QuerryFullPojo {
		String word;
		ArrayList<String> offset;
		ArrayList<String> fileName;

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public ArrayList<String> getOffset() {
			return offset;
		}

		public void setOffset(ArrayList<String> offset) {
			this.offset = offset;
		}

		public ArrayList<String> getFileName() {
			return fileName;
		}

		public void setFileName(ArrayList<String> fileName) {
			this.fileName = fileName;
		}

	}

/////////////////////////////////////////////////
////************* full position index phrase
////////////////////////////////////////
////mapper class beaking each word and converting it to word and (offset,docId) as key value pair ,which  stored on local and later used by reducer.Each posting is sorted internally

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private long numRecords = 0;
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			Text currentWord = new Text();
			Text currentWordTwo = new Text();
			String[] parts = WORD_BOUNDARY.split(line);

			for (int i = 0; i < parts.length; i++) {
				if (parts[i].isEmpty()) {
					continue;
				}
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				Text fileNameValue = new Text(fileName);
				currentWord = new Text(parts[i]);
				StringBuilder sb = new StringBuilder();
				sb.append(fileNameValue + " | " + offset);
				Text value = new Text(sb.toString());
				context.write(currentWord, value);
				if (i + 2 < parts.length) {
					currentWordTwo = new Text(parts[i] + " " + parts[i + 2]);
					context.write(currentWordTwo, value);
					System.out.println("word===:" + currentWordTwo + " posting=" + value);

				}
			}
		}
	}

/////each pair of key-value is given to a reducer 
////reducer breakes value as documentId and offset and stored as object which are sorted internally
///each object checked for duplicate ,if duplicate postings for similar document are combined
//object along with its word is written to file
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
	//// creating posting bean class so sorting and comparison is done easily and
	//// other computation

	public static class PostingPojo {
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
