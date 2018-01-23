/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Simple word count demo.
 */
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  private static final class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      Set<String> uniqueTokenSet = new HashSet<String>();

      for (int i = 0; i < tokens.size() && i < 40; i++) {
        if (uniqueTokenSet.add(tokens.get(i))){
          WORD.set(tokens.get(i));
          context.write(WORD, ONE);
        }
      }
    }
  }

  private static final class MyReducer1 extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class LineCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text LINE = new Text("line");

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(LINE, ONE);
    }
  }

  private static final class LineCountReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }


  public static final class MyMapper2 extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static Text KEY = new Text();
    private static HMapStIW MAP = new HMapStIW();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
          List<String> tokens = Tokenizer.tokenize(value.toString());
          Set<String> uniqueTokenSet = new HashSet<String>();
          String A = "";
          String B = "";

          for (int i = 0; i < tokens.size() && i < 40; i++) {
            uniqueTokenSet.add(tokens.get(i));
          }

          String[] uniqueTokenArray = new String[uniqueTokenSet.size()];
          uniqueTokenSet.toArray(uniqueTokenArray);

          for (int i = 0; i < uniqueTokenArray.length; i++){
            A = uniqueTokenArray[i];
            for (int j = i + 1; j < uniqueTokenArray.length; j++){
              B = uniqueTokenArray[j];

              if(!MAP.containsKey(B)){
                MAP.put(B , 1);
              }
            }
            KEY.set(A);
            context.write(KEY,MAP);
            MAP.clear();
          }
    }
  }

  private static class MyCombiner2 extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    private static HMapStIW numCoOccurences = new HMapStIW();

    @Override
    public void reduce(Text term, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException{
          // Element-wise sume of the maps
          for (HMapStIW mapValue: values){
            for (String key: mapValue.keySet()){
              if (numCoOccurences.containsKey(key)){
                numCoOccurences.put(key, numCoOccurences.get(key) + mapValue.get(key));
              } else {
                numCoOccurences.put(key, mapValue.get(key));
              }
            }
          }
      context.write(term, numCoOccurences);
      numCoOccurences.clear();
    }
  }

  public static final class MyReducer2 extends Reducer<Text, HMapStIW, Text, Text> {
    private int threshold = 0;
    private int numLines = 0;
    private static HMapStIW occurrence = new HMapStIW();
    private static HMapStIW numCoOccurences = new HMapStIW();
    private static PairOfFloatInt floatIntPair = new PairOfFloatInt();

    @Override
    public void setup(Context context) {
      threshold = context.getConfiguration().getInt("threshold", 0);
      Path lineCountFile = new Path("./lineCount/part-r-00000");
      Configuration conf = context.getConfiguration();

      try{
        FileSystem fs1 = FileSystem.get(conf);
        BufferedReader reader = null;
        FSDataInputStream in1 = fs1.open(lineCountFile);
        InputStreamReader inStream1 = new InputStreamReader(in1);
        reader = new BufferedReader(inStream1);
        String line = reader.readLine();
        numLines = Integer.parseInt(line.split("\\s+")[1]);
        reader.close();
      } catch(IOException e){
        e.printStackTrace();
      }

      Path intermediateCountFile = new Path("./intermediate/part-r-00000");
      try{
        FileSystem fs2 = FileSystem.get(conf);
        BufferedReader reader = null;
        FSDataInputStream in2 = fs2.open(intermediateCountFile);
        InputStreamReader inStream2 = new InputStreamReader(in2);
        reader = new BufferedReader(inStream2);
        String line = reader.readLine();
        while(line != null){
          String[] pair = line.split("\\s+");
          String A = pair[0];
          int count = Integer.parseInt(pair[1]);

          if (occurrence.containsKey(A)) {
            occurrence.put(A, occurrence.get(A) + count);
          } else {
            occurrence.put(A, count);
          }

          line = reader.readLine();
        }
        reader.close();
      } catch(IOException e){
        e.printStackTrace();
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {

      // Element-wise sume of the maps
      for (HMapStIW mapValue: values){
        for (String mappedkey: mapValue.keySet()){
          if (numCoOccurences.containsKey(mappedkey)){
            numCoOccurences.put(mappedkey, numCoOccurences.get(mappedkey) + mapValue.get(mappedkey));
          } else {
            numCoOccurences.put(mappedkey, mapValue.get(mappedkey));
          }
        }
      }

      for (String mappedkey: numCoOccurences.keySet()){
        if (numCoOccurences.get(mappedkey) >= threshold){

          double probPair = (float)numCoOccurences.get(mappedkey) / numLines;
          double probLeft = (float)occurrence.get(key.toString()) / numLines;
          double probRight = (float)occurrence.get(mappedkey) / numLines;
          double pmi = Math.log10(probPair / (probLeft * probRight));

          floatIntPair.set((float)pmi, numCoOccurences.get(mappedkey));
          context.write(key, new Text(mappedkey + ": " + floatIntPair.toString()));

        }
      }
      numCoOccurences.clear();

    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", required = true, usage = "threshold of co-occurrence")
    int threshold = 0;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    Job lineCountJob = Job.getInstance(conf);
    lineCountJob.setJobName(StripesPMI.class.getSimpleName());
    lineCountJob.setJarByClass(StripesPMI.class);

    lineCountJob.setNumReduceTasks(1);

    // Delete the output directory if it exists already.
    Path lineCountOutputDir = new Path("lineCount");
    FileSystem.get(conf).delete(lineCountOutputDir, true);

    FileInputFormat.setInputPaths(lineCountJob, new Path(args.input));
    FileOutputFormat.setOutputPath(lineCountJob, new Path("lineCount"));

    lineCountJob.setMapOutputKeyClass(Text.class);
    lineCountJob.setMapOutputValueClass(IntWritable.class);
    lineCountJob.setOutputKeyClass(Text.class);
    lineCountJob.setOutputValueClass(IntWritable.class);

    lineCountJob.setOutputFormatClass(TextOutputFormat.class);

    lineCountJob.setMapperClass(LineCountMapper.class);
    // lineCountJob.setCombinerClass(LineCountReducer.class);
    lineCountJob.setReducerClass(LineCountReducer.class);

    long lineCountJobStartTime = System.currentTimeMillis();
    lineCountJob.waitForCompletion(true);
    LOG.info("Line Count Job Finished in " + (System.currentTimeMillis() - lineCountJobStartTime) / 1000.0 + " seconds");

    Job job1 = Job.getInstance(conf);
    job1.setJobName(StripesPMI.class.getSimpleName());
    job1.setJarByClass(StripesPMI.class);

    job1.setNumReduceTasks(1);

    // Delete the output directory if it exists already.
    Path intermediateOutputDir = new Path("intermediate");
    FileSystem.get(conf).delete(intermediateOutputDir, true);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path("intermediate"));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(MyMapper1.class);
    // job1.setCombinerClass(MyReducer1.class);
    job1.setReducerClass(MyReducer1.class);


    long job1StartTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job1 Finished in " + (System.currentTimeMillis() - job1StartTime) / 1000.0 + " seconds");

    // Second Job

    Job job2 = Job.getInstance(conf);
    job2.setJobName(StripesPMI.class.getSimpleName());
    job2.setJarByClass(StripesPMI.class);

    job2.getConfiguration().setInt("threshold", args.threshold);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(HMapStIW.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    job2.setMapperClass(MyMapper2.class);
    // job2.setCombinerClass(MyCombiner2.class);
    job2.setReducerClass(MyReducer2.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    long job2StartTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    LOG.info("Job2 Finished in " + (System.currentTimeMillis() - lineCountJobStartTime) / 1000.0 + " seconds");


    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
