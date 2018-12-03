package ex1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Instructions – In-mapper and In-memory
 *
 * Starting from the basic example, the student should create new versions with the in-mapper and in-memory design
 * pattern. For the latter, the student will need to use the “setup” and cleanup” methods.
 * Once completed, the student should export the jar files and execute them on the container. The student should compare
 * the output messages provided by Hadoop with the different versions of the WordCount (basic, in-mapper and in-
 * memory).
 */
public class WordCountInMemory extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(WordCountInMemory.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCountInMemory(), args); //Viene avviata un'istanza di World Count (args: <file input>, <dir output>, <#reducer>)
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setNumReduceTasks(Integer.parseInt(args[2])); //se non lo mettiamo di default c'è 1 reducer
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    HashMap<String, Integer> H;

    /**
     * Questo metodo viene chiamato una volta sola per tutti i mapper, all'inizio
     * @param context
     */
    public void setup(Context context){
      H = new HashMap<>();
    }

    public void map(LongWritable offset, Text lineText, Context context){
      String line = lineText.toString();

      Integer oldValue = 0;

      //Analizzo la riga
      for (String word : WORD_BOUNDARY.split(line)) {
        if (word.isEmpty())
          continue;

        if (H.containsKey(word)){
          oldValue = H.get(word);
          H.remove(word);
        }
        else
          oldValue = 0;

        H.put(word, oldValue + 1);
      }
    }

    /**
     * Questo metodo viene chiamato una volta sola per tutti i reducer, alla fine
     * @param context
     */
    public void cleanup(Context context) throws IOException, InterruptedException{
      Text currentWord = new Text();
      for(String item : H.keySet()){
        currentWord.set(item);
        context.write(currentWord, new IntWritable(H.get(item)));
      }
    }
  }


  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}