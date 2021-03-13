package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            throw new RuntimeException(
                    "You should specify input, output folders and scale in format: \\d+[s|m|h|d|w]!"
            );
        }
        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        // передаём временной диапазон аггрегации для маппера
        conf.set("scale", args[2]);

        Job job = Job.getInstance(conf, "metrics' scores scaled counts");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
    }
}
