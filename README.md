import java.io.IOException;

public class ElectricMapper extends MapReduceBase implements Mapper<LongWritable, /*Input key Type*/
Text, /*Input value Type*/
Text, /*Output key Type*/
IntWritable /*Output value Type*/ 
{
//Map function
public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    String line = value.toString();
    String lasttoken = null;
    StringTokenizer s = new StringTokenizer(line, "\t");
    String year = s.nextToken();

    while(s.hasMoreTokens()) {
        lasttoken = s.nextToken();
    }

    int avgprice = Integer.parseInt(lasttoken);
    output.collect(new Text(year), new IntWritable(avgprice));
}
}


import java.io.IOException;

public abstract class ElectricReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    // Reduce function
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int maxavg = 30;
        int val = Integer.MIN_VALUE;
        while (values.hasNext()) {
            if ((val = values.next().get()) >= maxavg) {
                output.collect(key, new IntWritable(val));
            }
        }
    }
}


import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ElectricCalculator {
    public static void main(String args[]) throws Exception {
        JobConf conf = new JobConf(ElectricCalculator.class);
        
        conf.setJobName("unis_above_30");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapperClass(ElectricMapper.class);
        conf.setCombinerClass(ElectricReducer.class);
        conf.setReducerClass(ElectricReducer.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);
    }
}
