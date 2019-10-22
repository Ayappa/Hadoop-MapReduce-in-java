 
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
import org.apache.hadoop.mapreduce.Reducer.Context;
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

public class QueryIndex extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Index.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new QueryIndex(), args);
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
		if (flag == 1) {
			/// creating arraylist of objects for query mapReduce output
			ArrayList<QuerryFullPojo> index = querryProcessingPosting(new Path(args[1]));
			///// if u dont execute please run this one
			 Path path = new Path(new Path(args[2]) + "/part-r-00000");
			///// after running combine_file.py execute this line
			//Path path = new Path(new Path(args[2]) + "/combined_index");
			System.out.println("path===" + path);
			/// creating arraylist of objects for document mapReduce output
			ArrayList<QuerryFullPojo> querryList = querryProcessingPosting(path);
			compareQuerry(index, new Path(args[1]), querryList, path);
		}
		return 0;
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
		for (int i = 0; i < querryList.size(); i++) {
			StringBuilder sb = new StringBuilder();
			System.out.println("wordwordword====" + querryList.get(i).getWord());
			sb.append(querryList.get(i).getWord() + " : ");
			for (int j = 0; j < index.size(); j++) {
				if (querryList.get(i).getWord().equals(index.get(j).getWord())) {
					boolean flag = true;
					System.out.println("wordwordwordIndexIndexIndex====" + querryList.get(i).getWord());
					System.out.println("wordwordwordIndexIndexIndex====" + index.get(j).getWord());

					for (int m = 0; m < index.get(j).getFileName().size(); m++) {
						if (!flag) {
							sb.append(",");
							flag = true;
						}
						;
						sb.append(index.get(j).getFileName().get(m) + m);
						// sb.append(System.getProperty("line.separator"));
						flag = false;
					}
					sb.append(System.getProperty("line.separator"));

					try {
						filewriter.write(sb.toString());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(sb.toString());
					// writer.close();

				} else {
					continue;
				}
			}
			sb.setLength(0);

		}
		filewriter.close();

	}

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

	private ArrayList querryProcessingPosting(Path path) throws IOException {
		// TODO Auto-generated method stub
		String name = path.toString();
		BufferedReader br = null;
		ArrayList<QuerryFullPojo> querryList = new ArrayList<QuerryFullPojo>();
		try {
			br = new BufferedReader(new FileReader(name));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			String[] part = null;
			String[] parts = null;

			while (line != null) {
				ArrayList<String> offset = new ArrayList<String>();
				ArrayList<String> fileName = new ArrayList<String>();
				QuerryFullPojo querryfullpojo = new QuerryFullPojo();
				System.out.println("statwment---------------------" + line);
				part = line.split("[ : ]");
				parts = part[2].split(",");
				querryfullpojo.setWord(part[0]);
				System.out.println("//////////////");
				for (int i = 0; i < parts.length; i++) {
					System.out.println("statwment=" + parts[i]);
					fileName.add(parts[i]);
				}
				querryfullpojo.setFileName(fileName);
//				if (parts.length > ) {
//					for (int i = 1; i < parts.length; i = i + 2) {
//						System.out.println("statwment=" + parts[i]);
//						String[] partsres = parts[i].split("[|*|]");
//						System.out.println("//////////////");
//						fileName.add(partsres[1]);
//						// offset.add(partsres[2]);
//						querryfullpojo.setFileName(fileName);
//						querryfullpojo.setOffset(offset);
////				    	   System.out.println("statwmentssssssssssss="+partsres[1]);   
////				    	   System.out.println("statwmentssssssssssss="+partsres[2]);   
//
//					}
//					System.out.println(
//							"word=======" + querryfullpojo.getWord() + "file==" + querryfullpojo.getFileName());
//
//				} else {
//					String[] partsres = parts[1].split("[|*|]");
//					fileName.add(partsres[1]);
//					offset.add(partsres[2]);
//					querryfullpojo.setFileName(fileName);
//					// querryfullpojo.setOffset(offset);
//					System.out.println(
//							"word=======" + querryfullpojo.getWord() + "file==" + querryfullpojo.getFileName());
//				}
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

/////each pair of key is given to a reducer and reducer combines document ID for each word as postings and writes it to file .Each postin is sorted internally
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private long numRecords = 0;
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			Text currentWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				Text fileNameValue = new Text(fileName);

				currentWord = new Text(word);
				System.out.println("current word=" + currentWord + " filename=" + fileNameValue);
				context.write(currentWord, fileNameValue);
			}
		}
	}

/////each pair of key is given to a reducer and reducer combines document ID for each word as postings and writes it to file .Each postin is sorted internally
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)

				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			boolean flag = true;
			Text sumMulti = new Text("null");
			ArrayList<String> al = new ArrayList<String>();
			for (Text count : values) {
				System.out.println("current word=" + word + " filename=" + count);

				String[] parts = count.toString().split(".txt");
				if (sumMulti.toString().contains(parts[0])) {
				} else {
					al.add(parts[0]);
					sumMulti = new Text(parts[0]);
				}
			}
			Collections.sort(al);
			sb.append(": " + al.get(0) + ".txt");
			for (int i = 1; i < al.size(); i++) {
				System.out.println(al.get(i));
				sb.append("," + al.get(i) + ".txt");
			}
			sb.append(",");
			Text sum = new Text(sb.toString());
			context.write(word, sum);
		}
	}

}
