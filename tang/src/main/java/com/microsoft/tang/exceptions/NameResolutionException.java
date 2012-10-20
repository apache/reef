package com.microsoft.tang.exceptions;

public class NameResolutionException extends Exception {
    public NameResolutionException(String name, String longestPrefix) {
        super("Could not resolve " + name + ".  Search ended at prefix " + longestPrefix);
    }
}
