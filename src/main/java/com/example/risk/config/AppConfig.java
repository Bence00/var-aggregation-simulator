package com.example.risk.config;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Loads config.properties and provides a JDBC connection factory.
 * Single instance; pass it down to repositories and services.
 */
public class AppConfig {

    private final Properties props;
    private Integer schemaMaxAttributesOverride;

    public AppConfig() {
        props = new Properties();
        try (InputStream in = AppConfig.class.getResourceAsStream("/config.properties")) {
            if (in == null) throw new RuntimeException("config.properties not found on classpath");
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }
    }

    // ── Database ───────────────────────────────────────────────────────

    public String getDbUrl()      { return props.getProperty("db.url"); }
    public String getDbUsername() { return props.getProperty("db.username"); }
    public String getDbPassword() { return props.getProperty("db.password"); }

    /** Open a raw JDBC connection. Caller is responsible for closing it. */
    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(getDbUrl(), getDbUsername(), getDbPassword());
    }

    // ── Application defaults ───────────────────────────────────────────

    public String getDefaultDatasetId()    { return props.getProperty("default.dataset.id",  "DATASET_001"); }
    public int    getDefaultNumRecords()   { return Integer.parseInt(props.getProperty("default.num.records",  "5000")); }
    public int    getDefaultNumAttributes(){ return Integer.parseInt(props.getProperty("default.num.attributes", "5")); }
    public int    getDefaultCardinality()  { return Integer.parseInt(props.getProperty("default.attr.cardinality", "8")); }
    public int    getDefaultNumbersLength(){ return Integer.parseInt(props.getProperty("default.numbers.length", "1000")); }
    public long   getDefaultSeed()         { return Long.parseLong(props.getProperty("default.seed", "42")); }
    public String getDefaultPercentiles()  { return props.getProperty("default.percentiles", "1.0,95.0,99.0"); }
    public String getDefaultInteresting()  { return props.getProperty("default.interesting", "0,1,2"); }
    public int    getSchemaMaxAttributes() {
        if (schemaMaxAttributesOverride != null) {
            return schemaMaxAttributesOverride;
        }
        return Integer.parseInt(props.getProperty("schema.max.attributes", "16"));
    }

    public void setSchemaMaxAttributes(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("schema.max.attributes must be positive");
        }
        schemaMaxAttributesOverride = value;
    }
}
