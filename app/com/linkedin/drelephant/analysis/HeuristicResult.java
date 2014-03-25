package com.linkedin.drelephant.analysis;

import java.util.*;

public class HeuristicResult {
    public static final HeuristicResult SUCCESS = new HeuristicResult("Everything looks good", true);
    public static final List<String> possibleResults = new ArrayList<String>();
    public static final Set<String> possibleResultsSet = new HashSet<String>();

    public static String addPossibleMapperResult(String message) {
        return addPossibleResult(MapReduceSide.MAP, message);
    }

    public static String addPossibleReducerResult(String message) {
        return addPossibleResult(MapReduceSide.REDUCE, message);
    }

    private static String addPossibleResult(MapReduceSide side, String message) {
        message = side.getName() + message;
        possibleResultsSet.add(message);
        possibleResults.clear();
        possibleResults.addAll(possibleResultsSet);
        Collections.sort(possibleResults);
        return message;
    }

    private static enum MapReduceSide {
        MAP("Mapper side "),
        REDUCE("Reducer side ");

        private String name;

        MapReduceSide(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private String message;
    private List<String> details;
    private int detailsColumns = 0;
    private boolean success;

    public HeuristicResult(String message, boolean success) {
        this.message = message;
        this.details = new ArrayList<String>();
        this.success = success;
    }

    public boolean succeeded() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public List<String> getDetails() {
        return details;
    }

    public String getDetailsCSV() {
        StringBuilder sb = new StringBuilder();
        for (String line : details) {
            sb.append(line).append("\n");
        }
        return sb.toString().trim();
    }

    public int getDetailsColumns() {
        return detailsColumns;
    }

    public void addDetail(String... parts) {
        details.add(createLine(parts));
        if (parts.length > detailsColumns) {
            detailsColumns = parts.length;
        }
    }

    public static String createLine(String... parts) {
        StringBuilder sb = new StringBuilder();
        String quotes = "\"";
        String comma = ",";
        for (int i = 0; i < parts.length; i++) {
            sb.append(quotes).append(parts[i].replaceAll("\"", "\\\"")).append(quotes);
            if (i != parts.length - 1) {
                sb.append(comma);
            }
        }
        return sb.toString();
    }
}
