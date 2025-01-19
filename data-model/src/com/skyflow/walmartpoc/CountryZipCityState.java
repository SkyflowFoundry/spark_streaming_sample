package com.skyflow.walmartpoc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

class CountryZipCityState {
    String country;
    String zip;
    String city;
    String state;

    CountryZipCityState(String country, String zip, String city, String state) {
        this.country = country;
        this.zip = zip;
        this.city = city;
        this.state = state;
    }

    static List<CountryZipCityState> loadData(String resourcePath) throws IOException {
        List<CountryZipCityState> data = new ArrayList<>();
        try (InputStream is = CountryZipCityState.class.getResourceAsStream("/" + resourcePath);
                BufferedReader br = new BufferedReader(new InputStreamReader(is))) {

            String line;
            while ((line = br.readLine()) != null) {
                // Split on tabs
                String[] columns = line.split("\t");
                if (columns.length >= 4) {
                    // We only need the first four columns: country, zip, city, state
                    String country = columns[0];
                    String zip     = columns[1];
                    String city    = columns[2];
                    String state   = columns[3];
                    
                    data.add(new CountryZipCityState(country, zip, city, state));
                }
            }
        }
        return data;
    }
}
