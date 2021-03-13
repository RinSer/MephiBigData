package bdtc.lab1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.InputMismatchException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Кастомный тип метрик, используется как ключ
 * и для хранения информации не участвующей
 * непосредственно в вычислениях редьюсера
 */
public class MetricWritable implements WritableComparable<MetricWritable> {

    public final String separator = ", ";

    // идентификатор метрики
    private int metricId;
    // отметка вермени, поделенная на диапазон
    private int timestamp;
    // временной диапазон в виде строки
    public int scaleSize;
    public char scaleUnit;
    // временной диапазон в виде числа в секундах
    private int span;
    // значение метрики
    private long score;

    // Default constructor to allow (de)serialization
    MetricWritable() {}

    /**
     * При создании каждый объект должен получать
     * диапазон времени в качестве параметра
     * @param timeSpan диапазон времени
     * @throws InputMismatchException если диапазон времени не соответсвует шаблону [\d+(s|m|h|d|w)]
     */
    public MetricWritable(String timeSpan) throws InputMismatchException {
        if (stringMatchesPattern(timeSpan, "\\d+(s|m|h|d|w)")) {
            int last = timeSpan.length() - 1;
            span = Integer.parseInt(timeSpan.substring(0, last));
            scaleSize = span;
            scaleUnit = timeSpan.charAt(last);
            switch (timeSpan.substring(last)) {
                case "m":
                    span *= 60;
                    break;
                case "h":
                    span *= 3600;
                    break;
                case "d":
                    span *= (24 * 3600);
                    break;
                case "w":
                    span *= (7 * 24 * 3600);
                    break;
            }
        } else {
            throw new InputMismatchException("Scale should match regex [\\d+(s|m|h|d|w)]!");
        }
    }

    /**
     * Вспомогательный метод для проверки
     * соответствия строки шаблону
     * @param input строка, которую надо проверить
     * @param match проверочный шаблон в виде регулярного выражения
     * @return соответствует или нет
     */
    private boolean stringMatchesPattern(String input, String match) {
        Pattern pattern = Pattern.compile(match, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(input);
        return matcher.matches();
    }

    /**
     * Заполняет поля по строке из входного файла
     * @param line строка входного файла
     * @throws InputMismatchException если строка не совпадает шаблону
     */
    public void parse(String line) throws InputMismatchException {
        if (stringMatchesPattern(line, "\\d+,\\s\\d+,\\s\\d+")) {
            String[] cells = line.split(separator);
            metricId = Integer.parseInt(cells[0]);
            timestamp = Integer.parseInt(cells[1]) / span;
            score = Integer.parseInt(cells[2]);
        } else {
            throw new InputMismatchException("Each input line should match regex [\\d+, \\d+, \\d+]!");
        }
    }

    /**
     * Возвращает значение метрики
     * @return значение метрики
     */
    public long getScore() { return score; }

    /**
     * Возвращает ключ для аггрегации
     * @return название метрики + разделитель + отметка времени + разделитель + диапазон времени
     */
    public Text getKey() {
        String metricName = MetricsNames.getNameById(metricId);
        return new Text(metricName + separator + timestamp + separator + scaleSize + scaleUnit);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(metricId);
        out.writeInt(timestamp);
        out.writeInt(scaleSize);
        out.writeChar(scaleUnit);
        out.writeInt(span);
        out.writeLong(score);
    }

    public void readFields(DataInput in) throws IOException {
        metricId = in.readInt();
        timestamp = in.readInt();
        scaleSize = in.readInt();
        scaleUnit = in.readChar();
        span = in.readInt();
        score = in.readLong();
    }

    public int compareTo(MetricWritable other) {
        return this.metricId == other.metricId && this.timestamp == other.timestamp ? 0
                : this.timestamp < other.timestamp ? -1 : 1;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj.getClass() != MetricWritable.class)
            return false;
        if (obj == this)
            return true;
        MetricWritable other = (MetricWritable)obj;
        return this.metricId == other.metricId
            && this.timestamp == other.timestamp;
    }
}
