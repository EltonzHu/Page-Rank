/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with Hadoop framework for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Doing some pre-processing data work: Cleaning and split data into "From To" format
            String[] fromTo = value.toString().trim().split("\t");

            if (fromTo.length == 1 || fromTo[1].trim().equals("")) {
                /*
                 * Corner case 1: Only from, missing to portion
                 * Corner case 2: To portion is an Empty String
                 */
                return;
            }

            String[] tos = fromTo[1].split(",");
            for (String to: tos) {
                /*
                 * Note that, "_" is used between to and probability values.
                 * This character "_" will be used in the reducer to identify which data coming from which mapper.
                 *
                 * - Elton Hu (Sept.28, 2018)
                 */
                context.write(new Text(fromTo[0]), new Text(to + "_" + (double) 1 / tos.length));
            }
        }
    }

    public static class PrMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            double prValue = 0.0;
            List<String> transitionValues = new ArrayList<String>();

            // Distinguish input data
            for (Text value: values) {
                if (value.toString().contains("_")) {
                    // From TransitionMapper
                    transitionValues.add(value.toString());
                } else {
                    // From PrMapper
                    prValue = Double.parseDouble(value.toString());
                }
            }

            for (String value: transitionValues) {
                String[] toProb = value.split("_");
                double prob = Double.parseDouble(toProb[1]);
                String outputKey = toProb[0];
                String outputValue = String.valueOf(prob * prValue);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main (String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(UnitMultiplication.class);

        /*
         * Note:
         * There is another approach by using ChainMapper as shown below:
         *   ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
         *   ChainMapper.addMapper(job, PrMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
         *
         * ChainMapper is smart enough knowing those two mappers are not chaining together by checking input parameters.
         * Those two mappers are in parallel.
         *
         * However, ChainMapper approach in this case is not clear compared to using setMapperClass API which is easy
         * for future maintenance.
         *
         * - Elton Hu (Sept.28, 2018)
         */
        job.setMapperClass(TransitionMapper.class);
        job.setMapperClass(PrMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PrMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
