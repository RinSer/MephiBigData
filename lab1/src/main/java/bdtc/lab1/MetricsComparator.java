package bdtc.lab1;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableComparator;

/**
 * Сравниватель объектов метрик, используется для установления
 * тождественности и сортировки, тестируется вместе с МапРедьюсером
 */
public class MetricsComparator extends WritableComparator {

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            DataInputBuffer buffer1 = new DataInputBuffer();
            buffer1.reset(b1, s1, l1);

            int metricId1 = buffer1.readInt();
            int timestamp1 = buffer1.readInt();

            DataInputBuffer buffer2 = new DataInputBuffer();
            buffer2.reset(b2, s2, l2);

            int metricId2 = buffer2.readInt();
            int timestamp2 = buffer2.readInt();

            int compare = Integer.compare(metricId1, metricId2);
            if (compare == 0) {
                compare += Integer.compare(timestamp1, timestamp2);
            }

            return compare;
        } catch (Exception ex) {
            return -1;
        }
    }
}