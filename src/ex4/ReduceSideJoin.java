/**
 **/

package ex4;

import ex2.TextPair;

import java.awt.image.AreaAveragingScaleFilter;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * SCOPO: TORVARE GLI AMICI DEGLI AMICI
 */
public class ReduceSideJoin extends Configured implements Tool {
	private Path outputDir;
	private Path inputPath;
	private int numReducers;

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = new Job (conf);
		job.setJobName("ReduceSideJoin");

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RSMap.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(RSReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(RSPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setNumReduceTasks(numReducers);

		job.setJarByClass(ReduceSideJoin.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public ReduceSideJoin(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: ReduceSideJoin <num_reducers> <input_file> <output_dir>");
			System.exit(0);
		}
		this.numReducers = Integer.parseInt(args[0]);
		this.inputPath = new Path(args[1]);
		this.outputDir = new Path(args[2]);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(args), args);
		System.exit(res);
	}

}

class RSMap extends Mapper<LongWritable, Text, TextPair, Text> {

	static final public String TAG_LEFT = "0";
	static final public String TAG_RIGHT = "1";

	private TextPair joinAttr = new TextPair();
	private Text column = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lista = line.split("\t");
		String left = lista[0];
		String right = lista[1];

		joinAttr.set(new Text(right), new Text(TAG_RIGHT));
		column.set(left);
		context.write(joinAttr, column);

		joinAttr.set(new Text(left), new Text(TAG_LEFT));
		column.set(right);
		context.write(joinAttr, column);
	}
}

class RSReduce extends Reducer<TextPair, Text, Text, Text> {
	static final public String TAG_LEFT = "0";
	//private List<Text> leftTable = new LinkedList<>();
	private HashMap<Text, ArrayList<Text>> leftTable = new HashMap<>();
	private Text outR = new Text();
	private Text outL = new Text();

	@Override
	protected void reduce(TextPair joinAttr, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if(joinAttr.getSecond().equals(new Text(TAG_LEFT))){ //CASO LEFT --> VALORI DA CONSERVARE
			ArrayList<Text> listOfHisFriends = new ArrayList<>();
			for (Text friendL: values) {
				listOfHisFriends.add(new Text(friendL));
			}
			leftTable.put(joinAttr.getFirst(), listOfHisFriends);
		}
		else{ //CASO RIGHT --> VALORI DI CUI FARE L'EMIT
			ArrayList<Text> listOfHisFriends = leftTable.get(joinAttr.getFirst());

			for (Text friendR: values) {
				for (Text friendL: listOfHisFriends) {
					outL.set(friendL);
					outR.set(friendR);
					context.write(outL, outR);
				}
			}
		}
	}

	/*public static void logger(Context c, String s) throws IOException, InterruptedException{
		c.write(new Text(s), new Text(""));
	}*/
}

class RSPartitioner extends Partitioner<TextPair, Text> {
	@Override
	public int getPartition(TextPair key, Text value, int numPartitions) {
		return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}