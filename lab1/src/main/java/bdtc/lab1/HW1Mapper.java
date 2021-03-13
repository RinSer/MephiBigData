package bdtc.lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.InputMismatchException;

/**
 * Маппер: получает из конфигурации временной диапазон аггрегации,
 * затем создаёт экземпляр кастомного класса, который парсит строку файла,
 * а далее передаётся как ключ редьюсеру.
 */
public class HW1Mapper extends Mapper<LongWritable, Text, MetricWritable, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, InputMismatchException {
        Configuration conf = context.getConfiguration();
        MetricWritable metric = new MetricWritable(conf.get("scale"));
        String line = value.toString();
        metric.parse(line);
        context.write(metric, new LongWritable(metric.getScore()));
    }
}
