package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер: суммирует все показатели метрик, полученные от {@link HW1Mapper},
 * выдаёт суммарное цифры метрик в срезе минуты
 */
public class HW1Reducer extends Reducer<MetricWritable, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(MetricWritable metricWithStamp, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
        }

        context.write(
                new Text(metricWithStamp.getKey()),
                new LongWritable(sum)
        );
    }
}
