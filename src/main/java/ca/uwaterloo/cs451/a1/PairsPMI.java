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
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static final class MyMapper1 extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final PairOfStrings BIGRAM = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      Set<String> uniqueTokenSet = new HashSet<String>();
      String A = "";
      String B = "";

      for (int i = 0; i < tokens.size() && i < 40; i++) {
        if (uniqueTokenSet.add(tokens.get(i))){
          BIGRAM.set(tokens.get(i), " ");
          context.write(BIGRAM, ONE);
        }
      }

      String[] uniqueTokenArray = new String[uniqueTokenSet.size()];
      uniqueTokenSet.toArray(uniqueTokenArray);

      for (int i = 0; i < uniqueTokenArray.length; i++){
        for (int j = i + 1; j < uniqueTokenArray.length; j++){
          A = uniqueTokenArray[i];
          B = uniqueTokenArray[j];

          BIGRAM.set(A, B);
          context.write(BIGRAM, ONE);
        }
      }
    }
  }

  private static final class MyReducer1 extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
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

  private static final class MyPartitioner1 extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public static final class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, PairOfFloatInt> {
    private static PairOfFloatInt PMIandCOUNT = new PairOfFloatInt();
    private static PairOfStrings BIGRAM = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
          String stringCountPair = value.toString();

          String pairStr = stringCountPair.substring(stringCountPair.indexOf("(") + 1, stringCountPair.lastIndexOf(")"));

          float count = Float.parseFloat(stringCountPair.substring(stringCountPair.lastIndexOf(")") + 1).replaceAll("\\s+", ""));

          String[] pair = pairStr.split(",");
          BIGRAM.set(pair[0].replaceAll("\\s+", ""),pair[1].replaceAll("\\s+", ""));
          PMIandCOUNT.set(0, (int)count);
          if (!BIGRAM.getRightElement().equals("")){
            context.write(BIGRAM, PMIandCOUNT);
          }
    }
  }

  private static class MyCombiner2 extends Reducer<PairOfStrings, PairOfFloatInt, PairOfStrings, PairOfFloatInt> {
    private static PairOfFloatInt PMIandCOUNT = new PairOfFloatInt();

    @Override
    public void reduce(PairOfStrings pair, Iterable<PairOfFloatInt> values, Context context)
        throws IOException, InterruptedException{
      int sum = 0;
      for(PairOfFloatInt value : values){
        sum += value.getRightElement();
      }
      PMIandCOUNT.set(0, sum);
      context.write(pair, PMIandCOUNT);
    }
  }

  private static final class MyPartitioner2 extends Partitioner<PairOfStrings, PairOfFloatInt> {
    @Override
    public int getPartition(PairOfStrings key, PairOfFloatInt value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  // Reducer: sums up all the counts.
  public static final class MyReducer2 extends Reducer<PairOfStrings, PairOfFloatInt, PairOfStrings, PairOfFloatInt> {
    // Reuse objects.
    private int threshold = 0;
    private int numLines = 0;
    private static Map<String, Integer> occurrence = new HashMap<String, Integer>();
    private static Map<String, Integer> cooccurrence = new HashMap<String, Integer>();
    private static PairOfFloatInt PMIandCOUNT = new PairOfFloatInt();

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
          String pairStr = line.substring(line.indexOf("(") + 1, line.lastIndexOf(")"));
          float count = Float.parseFloat(line.substring(line.lastIndexOf(")") + 1).replaceAll("\\s+", ""));
          String[] pair = pairStr.split(",");
          String A = pair[0].replaceAll("\\s+", "");
          String B = pair[1].replaceAll("\\s+", "");

          if (B.equals("")){
            if (occurrence.containsKey(A)) {
              occurrence.put(A, occurrence.get(A) + (int)count);
            } else {
              occurrence.put(A, (int)count);
            }
          } else {
            if (cooccurrence.containsKey(A + " " + B)) {
              cooccurrence.put(A+" "+B, cooccurrence.get(A + " " + B)+(int)count);
            } else {
              cooccurrence.put(A+" "+B, (int)count);
            }
          }
          line = reader.readLine();
        }
        reader.close();
      } catch(IOException e){
        e.printStackTrace();
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfFloatInt> values, Context context)
        throws IOException, InterruptedException {

      PairOfStrings stringPair = new PairOfStrings();
      PairOfFloatInt floatIntPair = new PairOfFloatInt();


      int numCoOccurences = 0;
      for(PairOfFloatInt value : values) {
       numCoOccurences += value.getRightElement();
      }

      if (numCoOccurences >= threshold){
        String left = key.getLeftElement();
        String right = key.getRightElement();

        double probPair = (float)numCoOccurences / numLines;
        double probLeft = (float)occurrence.get(left) / numLines;
        double probRight = (float)occurrence.get(right) / numLines;
        double pmi = Math.log10(probPair / (probLeft * probRight));

        floatIntPair.set((float)pmi, numCoOccurences);

        context.write(key, floatIntPair);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

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

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    Job lineCountJob = Job.getInstance(conf);
    lineCountJob.setJobName(PairsPMI.class.getSimpleName());
    lineCountJob.setJarByClass(PairsPMI.class);

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
    lineCountJob.setReducerClass(LineCountReducer.class);
    lineCountJob.setCombinerClass(LineCountReducer.class);

    // Altiscale requirements
    lineCountJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    lineCountJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    lineCountJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    lineCountJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    lineCountJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    long lineCountJobStartTime = System.currentTimeMillis();
    lineCountJob.waitForCompletion(true);
    LOG.info("Line Count Job Finished in " + (System.currentTimeMillis() - lineCountJobStartTime) / 1000.0 + " seconds");

    Job job1 = Job.getInstance(conf);
    job1.setJobName(PairsPMI.class.getSimpleName());
    job1.setJarByClass(PairsPMI.class);

    job1.setNumReduceTasks(1);

    // Delete the output directory if it exists already.
    Path intermediateOutputDir = new Path("intermediate");
    FileSystem.get(conf).delete(intermediateOutputDir, true);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path("intermediate"));

    job1.setMapOutputKeyClass(PairOfStrings.class);
    job1.setMapOutputValueClass(FloatWritable.class);
    job1.setOutputKeyClass(PairOfStrings.class);
    job1.setOutputValueClass(FloatWritable.class);

    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(MyMapper1.class);
    job1.setCombinerClass(MyReducer1.class);
    job1.setReducerClass(MyReducer1.class);
    job1.setPartitionerClass(MyPartitioner1.class);

    // Altiscale requirements
    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    long job1StartTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job1 Finished in " + (System.currentTimeMillis() - job1StartTime) / 1000.0 + " seconds");

    // Second Job

    Job job2 = Job.getInstance(conf);
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);

    job2.getConfiguration().setInt("threshold", args.threshold);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path("intermediate"));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(PairOfFloatInt.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloatInt.class);

    job2.setMapperClass(MyMapper2.class);
    job2.setCombinerClass(MyCombiner2.class);
    job2.setReducerClass(MyReducer2.class);
    job2.setPartitionerClass(MyPartitioner2.class);

    // Altiscale requirements
    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

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
    ToolRunner.run(new PairsPMI(), args);
  }
}
