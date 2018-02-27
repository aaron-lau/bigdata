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

package ca.uwaterloo.cs451.a4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, PageRankMultiSourceNode, IntWritable, PageRankMultiSourceNode> {
    IntWritable PAGERANK = new IntWritable(0);
    private String[] source;

    @Override
    public void setup(Context context) throws IOException {
      source = context.getConfiguration().getStrings("source", "");
    }

    @Override
    public void map(IntWritable nid, PageRankMultiSourceNode node, Context context) throws IOException,
        InterruptedException {
          for (int i = 0; i < source.length; i++) {
            PAGERANK.set(i);
            context.write(PAGERANK, node);
          }
    }
  }

  private static class MyReducer extends
      Reducer<IntWritable, PageRankMultiSourceNode, FloatWritable, IntWritable> {
    private int top;
    private static final ArrayList sources = new ArrayList<Integer>();
    TopScoredObjects<Integer> topQueue;

    private final static IntWritable NODEID = new IntWritable();
		private final static FloatWritable PAGERANK = new FloatWritable();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String[] sourceList = conf.getStrings("source", "");
      for (int i = 0; i < sourceList.length; i++) {
        sources.add(Integer.parseInt(sourceList[i]));
      }
      top = conf.getInt("top", 0);
      topQueue = new TopScoredObjects<Integer>(top);
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankMultiSourceNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<PageRankMultiSourceNode> iter = iterable.iterator();
      while (iter.hasNext()){
        PageRankMultiSourceNode thisNode = iter.next();
        topQueue.add(thisNode.getNodeId(), thisNode.getPageRank(nid.get()));
      }

      System.out.println("Source: " + sources.get(nid.get()));
      for (PairOfObjectFloat<Integer> pair : topQueue.extractAll()) {

        int nodeid = ((Integer) pair.getLeftElement());
        float pagerank = (float) Math.exp(pair.getRightElement());
        System.out.println(String.format("%.5f %d", pagerank, nodeid));
        NODEID.set(nodeid);
        PAGERANK.set(pagerank);

        context.write(PAGERANK, NODEID);
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("top").hasArg()
        .withDescription("top").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int top = Integer.parseInt(cmdline.getOptionValue(TOP));
    String source = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + top);
    LOG.info(" - source: " + source);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("top", top);
    conf.setStrings("source", source);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks((job.getConfiguration().getStrings("source")).length);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankMultiSourceNode.class);

    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
