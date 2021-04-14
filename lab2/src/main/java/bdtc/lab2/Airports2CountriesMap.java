package bdtc.lab2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Отображатель аэропортов в страны
 * Загружает пары названий аэропортов и стран,
 * разделенные табом из файла, затем преобразует
 * их в хешмэп и предоставляет метод,
 * возвращающий название страны по названию аэропорта
 */
public class Airports2CountriesMap implements Serializable {

    final Map<String, String> airports2countries;

    public Airports2CountriesMap(String filePath) throws IOException {
        Path inputPath = FileSystems.getDefault().getPath(filePath);
        airports2countries = Files
                .lines(inputPath, StandardCharsets.UTF_16)
                .collect(Collectors.groupingBy((String l) -> l.split("\t")[0]))
                .values()
                .stream()
                .collect(Collectors.toMap(k -> k.get(0).split("\t")[0], v -> v.get(0).split("\t")[1]));
    }

    public String getCountry(String airport) {
        return airports2countries.get(airport);
    }
}