package bdtc.lab1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Кастомный тип метрик со значением
 */
public class MetricWritable implements WritableComparable<MetricWritable> {

    public final String scale = "1m";
    public final String separator = ", ";

    private int metricId;
    private int timestamp;
    private long score;

    // Default constructor to allow (de)serialization
    public MetricWritable() {}

    public void write(DataOutput out) throws IOException {
        out.writeInt(metricId);
        out.writeInt(timestamp);
        out.writeChars(scale);
        out.writeLong(score);
    }

    public void readFields(DataInput in) throws IOException {
        metricId = in.readInt();
        timestamp = in.readInt();
        score = in.readInt();
    }

    public int compareTo(MetricWritable other) {
        return this.metricId == other.metricId && this.timestamp == other.timestamp ? 0
                : this.timestamp < other.timestamp ? -1 : 1;
    }

    /**
     * Заполняет поля по строке из входного файла
     * @param line
     * @throws IOException
     */
    public void parse(String line) throws IOException {
        if (line.length() > 0) {
            String[] cells = line.split(separator);
            metricId = Integer.parseInt(cells[0]);
            timestamp = Integer.parseInt(cells[1]) / 60;
            score = Integer.parseInt(cells[2]);
        }
    }

    /**
     * Возвращает значение метрики
     * @return
     */
    public long getScore() { return score; }

    /**
     * Возвращает ключ для аггрегации:
     * ид метрики + отметка времени
     * @return
     */
    public Text getKey() {

        return new Text(
                metricId + separator + timestamp + separator + scale
        );
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        MetricWritable other = MetricWritable.class.cast(obj);
        return this.metricId == other.metricId
            && this.timestamp == other.timestamp;
    }
}
