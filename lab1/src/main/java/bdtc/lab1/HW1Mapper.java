package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class HW1Mapper extends Mapper<LongWritable, Text, MetricWritable, LongWritable> {

    private MetricWritable metric = new MetricWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.length() > 0) {
            metric.parse(line);
            context.write(metric, new LongWritable(metric.getScore()));
        }
    }
}
