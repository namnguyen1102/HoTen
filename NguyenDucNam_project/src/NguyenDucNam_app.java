import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DienTieuThu {

    public static class ConsumptionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable consumptionValue = new IntWritable();
        private Text year = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length > 1) {
                year.set(parts[0]); 
                int lastValue = Integer.parseInt(parts[parts.length - 1]); 
                consumptionValue.set(lastValue);
                context.write(year, consumptionValue);
            }
        }
    }

    public static class ConsumptionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable result = new IntWritable();
        private final static int threshold = 30;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                if (val.get() > threshold) {
                    result.set(val.get());
                    context.write(key, result);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "electricity consumption analysis");
        job.setJarByClass(DienTieuThu.class);
        job.setMapperClass(ConsumptionMapper.class);
        job.setReducerClass(ConsumptionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
