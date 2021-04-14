package bdtc.lab2;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

public class Airports2CountriesMapTests {

    final String airportsFilePath = "../app/data/airports2countries.txt";

    private Airports2CountriesMap mapToTest;

    @Before
    public void initiateTestMap() throws IOException {
        mapToTest = new Airports2CountriesMap(airportsFilePath);
    }

    @Test
    public void shouldMapAllAirportsToCountries() throws IOException {
        Path filePath = FileSystems.getDefault().getPath(airportsFilePath);
        String[] distinctLines = Files.lines(filePath, StandardCharsets.UTF_16)
                .collect(Collectors.groupingBy((String l) -> l.split("\t")[0]))
                .values().stream().map(lines -> lines.get(0)).toArray(String[]::new);
        for (String line : distinctLines)
        {
            String[] parts = line.split("\t");
            Assert.assertEquals("Contains each airport to country mapping",
                    parts[1], mapToTest.getCountry(parts[0]));
        }
    }

}
