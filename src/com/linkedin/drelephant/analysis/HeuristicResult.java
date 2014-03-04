package com.linkedin.drelephant.analysis;

public class HeuristicResult {
    public static final HeuristicResult SUCCESS = new HeuristicResult("Everything looks good", true);

    private String message;
    private boolean success;

    public HeuristicResult(String message, boolean success) {
        this.message = message;
        this.success = success;
    }

    public boolean succeeded() {
        return success;
    }

    public String getMessage() {
        return message;
    }
}
