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

package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.VIntWritable;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import tl.lin.data.pair.PairOfStringInt;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, VIntWritable> {
    private static final VIntWritable VINT = new VIntWritable();
    private static final PairOfStringInt WORDandDOCNO = new PairOfStringInt();

    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {

        // as discussed in clas place the doc number in the key
        VINT.set(e.getRightElement() );
        WORDandDOCNO.set(e.getLeftElement(), (int)docno.get());

        context.write(WORDandDOCNO, VINT);
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, VIntWritable, Text, BytesWritable> {
    private static final IntWritable DF = new IntWritable();
    private static Text prevText ;
    private static ArrayListWritable<PairOfInts> postingList ;
    private static IntWritable previousDocnum;
    private static BytesWritable bwritable ;
    private static ByteArrayOutputStream bytearray ;
    private static DataOutputStream data ;

    @Override
    public void setup(Context context)
      throws IOException, InterruptedException {
        postingList = new ArrayListWritable<PairOfInts>();
        previousDocnum = new IntWritable();
        previousDocnum.set(0);
        prevText = new Text("");
        bwritable = new BytesWritable();
        bytearray = new ByteArrayOutputStream();
        data = new DataOutputStream(bytearray);
    }

    @Override
    public void reduce(PairOfStringInt key, Iterable<VIntWritable> values, Context context)
        throws IOException, InterruptedException {
      ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

      String term = key.getLeftElement();

      // if key.term != prev and prev != null
      if ((!term.equals(prevText.toString())) && (prevText.getLength() != 0 )){

        for (PairOfInts pair : postingList){
          WritableUtils.writeVInt(data, pair.getLeftElement() ); //key.doc id
          WritableUtils.writeVInt(data, pair.getRightElement()); //term frequency
        }

        bwritable.set(bytearray.toByteArray(), 0, bytearray.size());
        // emit(key.term, postings)
        context.write(prevText, bwritable);
        // postings.reset()
        postingList.clear();
        bwritable.setSize(0);
        bytearray.reset();
        data.flush();
        previousDocnum.set(0);
      }

      Iterator<VIntWritable> iter = values.iterator();
      int df = 0;
      while (iter.hasNext()) {
        df = df + iter.next().get();
      }
      postingList.add(new PairOfInts(key.getRightElement() - (int)previousDocnum.get() , df));

      prevText.set(key.getLeftElement());
      previousDocnum.set(key.getRightElement());
    }

    // emit (prev, postings)
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        for (PairOfInts pair : postingList){
          WritableUtils.writeVInt(data, pair.getLeftElement()  ); //left doc num
          WritableUtils.writeVInt(data, pair.getRightElement()); //right term frequency
        }

        bwritable.set(bytearray.toByteArray(), 0, bytearray.size());

        int currDocnum = (int)previousDocnum.get();
        if (currDocnum != 0 ){
          context.write(prevText, bwritable);
        }

        postingList.clear();
        bytearray.reset();
        data.flush();
        bwritable.setSize(0);
        prevText.set("");

      }
    }


  protected static class MyPartitioner extends Partitioner<PairOfStringInt, VIntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, VIntWritable value, int numReduceTasks) {
      return (key.getLeftElement().toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(VIntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
