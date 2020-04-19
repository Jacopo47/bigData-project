package utils;

import java.util.ArrayList;
import java.util.List;

public class Flight {

    private final int year;
    private final String iataCode;
    private final int distance;
    private final int arrivalDelay;
    private final boolean cancelled;

    public Flight(int year, String iataCode, int distance, int arrivalDelay, boolean cancelled) {
        this.year = year;
        this.iataCode = iataCode;
        this.distance = distance;
        this.arrivalDelay = arrivalDelay;
        this.cancelled = cancelled;
    }

    public static Flight extract(String row) {
        List<String> columns = new ArrayList<>();
        for (String col : row.split(",")) {
            columns.add(col.replaceAll("\"", ""));
        }

        int year = getInt(columns.get(0));
        String airline = columns.get(4);
        int distance = getInt(columns.get(17));
        int arrivalDelay = getInt(columns.get(22));
        boolean cancelled = getBoolean(columns.get(24));

        return new Flight(year, airline, distance, arrivalDelay, cancelled);
    }

    private static int getInt(String str) {
        if (str.isEmpty()) {
            return 0;
        } else {
            return Integer.parseInt(str);
        }
    }

    private static boolean getBoolean(String str) {
        return !str.isEmpty() && !str.equals("0");
    }


    public int getYear() {
        return year;
    }

    public String getIataCode() {
        return iataCode;
    }

    public int getDistance() {
        return distance;
    }

    public int getArrivalDelay() {
        return arrivalDelay;
    }

    public boolean isCancelled() {
        return cancelled;
    }
}

