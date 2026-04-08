package com.example.risk;

import com.example.risk.ui.RiskApp;

/**
 * Application entry point.
 *
 * A separate launcher class is needed when running without a module-info.java
 * because JavaFX Application subclasses cannot be the main class on the
 * classpath in Java 11+.
 */
public class Main {
    public static void main(String[] args) {
        RiskApp.launch(RiskApp.class, args);
    }
}
