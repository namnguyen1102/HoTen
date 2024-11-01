import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ElectricityMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text nam = new Text();
    private LongWritable tieuThu = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] truong = value.toString().split(",");
        nam.set(truong[0]);
        long tong = 0;
        int dem = 0;
        for (int i = 1; i < truong.length - 1; i++) {
            tong += Long.parseLong(truong[i]);
            dem++;
        }
        long trungBinh = tong / dem;
        tieuThu.set(trungBinh);
        context.write(nam, tieuThu);
    }
}


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ElectricityReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable ketQua = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable val : values) {
            if (val.get() > 30) {
                ketQua.set(val.get());
                context.write(key, ketQua);
            }
        }
    }
}


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ElectricityDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tieu thu dien");
        job.setJarByClass(ElectricityDriver.class);
        job.setMapperClass(ElectricityMapper.class);
        job.setReducerClass(ElectricityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
