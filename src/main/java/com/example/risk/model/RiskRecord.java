package com.example.risk.model;

import java.util.List;

/**
 * Core data entity. Maps to the risk_records table.
 * numbers[] holds a scenario P&L vector for this record.
 */
public class RiskRecord {

    private Long id;
    private String datasetId;
    private List<String> attributes;
    private double[] numbers;

    public RiskRecord() {}

    public RiskRecord(String datasetId, List<String> attributes, double[] numbers) {
        this.datasetId  = datasetId;
        this.attributes = attributes;
        this.numbers    = numbers;
    }

    public Long getId()                  { return id; }
    public void setId(Long id)           { this.id = id; }

    public String getDatasetId()                     { return datasetId; }
    public void   setDatasetId(String datasetId)     { this.datasetId = datasetId; }

    public List<String> getAttributes()                      { return attributes; }
    public void         setAttributes(List<String> attrs)    { this.attributes = attrs; }

    public double[] getNumbers()                   { return numbers; }
    public void     setNumbers(double[] numbers)   { this.numbers = numbers; }
}
