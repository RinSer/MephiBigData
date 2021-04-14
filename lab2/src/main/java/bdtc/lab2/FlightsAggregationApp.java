package bdtc.lab2;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import static org.apache.spark.sql.functions.udf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class FlightsAggregationApp {

    public static void main(String[] args) throws TimeoutException, IOException, StreamingQueryException {

        Logger.getLogger("org")
            .setLevel(Level.OFF);
        Logger.getLogger("akka")
            .setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("FlightsAggregationApp");
        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "flights")
                .option("kafka.group.id", "use_a_separate_group_id_for_each_stream")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> flights = aggregateFlights(kafkaStream);

        StreamingQuery query = flights
                .writeStream()
                //.format("console")
                .option("checkpointLocation", "/tmp/flights_check_points/")
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "flights")
                .option("table", "counts")
                .outputMode(OutputMode.Append())
                //.trigger(Trigger.Continuous("1 second"))
                .start();

        query.awaitTermination();
    }

    public static Dataset<Row> aggregateFlights(Dataset<Row> stream) throws IOException {
        Airports2CountriesMap airports2Countries = new Airports2CountriesMap("../app/data/airports2countries.txt");
        UserDefinedFunction airport2country = udf((UDF1<String, Object>) airports2Countries::getCountry, DataTypes.StringType);

        UserDefinedFunction convertBytes = udf((byte[] record) ->
                new String(record, StandardCharsets.UTF_16), DataTypes.StringType);

        StructType scheme = new StructType()
                .add("number", DataTypes.StringType)
                .add("time", DataTypes.StringType)
                .add("departure", DataTypes.StringType)
                .add("arrival", DataTypes.StringType);

        Dataset<Row> flights = stream
                .select(convertBytes.apply(stream.col("value")).alias("value"));

        flights = flights
                .select(functions.from_json(flights.col("value"), scheme).alias("value"))
                .select("value.*");

        flights = flights
                .drop(flights.col("number"))
                .withColumn("time", functions.to_timestamp(
                        functions.split(flights.col("time"), ":").getItem(0), "yyyy-MM-dd'T'HH"))
                .withColumn("departure", airport2country.apply(flights.col("departure")))
                .withColumn("arrival", airport2country.apply(flights.col("arrival")));

        flights = flights
                .withWatermark("time", "60 minutes")
                .groupBy(
                        flights.col("time"),
                        flights.col("departure"),
                        flights.col("arrival")
                )
                .agg(
                        functions.count(functions.lit(1)).alias("count")
                );

        return flights
                .withWatermark("time", "60 minutes");
    }
}