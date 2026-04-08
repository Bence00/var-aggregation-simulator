package com.example.risk.generator;

import com.example.risk.generator.distribution.Distribution;

import java.util.List;

/**
 * Immutable configuration for synthetic data generation.
 * Build with {@link Builder}.
 */
public final class GeneratorConfig {

    private final String             datasetId;
    private final int                numRecords;
    private final int                numAttributes;
    private final int                attributeCardinality; // distinct values per attribute
    private final int                numbersLength;
    private final long               seed;
    private final List<Distribution> distributions; // one is picked at random per record

    private GeneratorConfig(Builder b) {
        this.datasetId           = b.datasetId;
        this.numRecords          = b.numRecords;
        this.numAttributes       = b.numAttributes;
        this.attributeCardinality= b.attributeCardinality;
        this.numbersLength       = b.numbersLength;
        this.seed                = b.seed;
        this.distributions       = List.copyOf(b.distributions);
    }

    public String             getDatasetId()           { return datasetId; }
    public int                getNumRecords()          { return numRecords; }
    public int                getNumAttributes()       { return numAttributes; }
    public int                getAttributeCardinality(){ return attributeCardinality; }
    public int                getNumbersLength()       { return numbersLength; }
    public long               getSeed()                { return seed; }
    public List<Distribution> getDistributions()       { return distributions; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        String             datasetId           = "DATASET_001";
        int                numRecords          = 5000;
        int                numAttributes       = 5;
        int                attributeCardinality= 8;
        int                numbersLength       = 1000;
        long               seed               = 42L;
        List<Distribution> distributions;

        public Builder datasetId(String v)           { datasetId = v;            return this; }
        public Builder numRecords(int v)             { numRecords = v;           return this; }
        public Builder numAttributes(int v)          { numAttributes = v;        return this; }
        public Builder attributeCardinality(int v)   { attributeCardinality = v; return this; }
        public Builder numbersLength(int v)          { numbersLength = v;        return this; }
        public Builder seed(long v)                  { seed = v;                 return this; }
        public Builder distributions(List<Distribution> v) { distributions = v; return this; }

        public GeneratorConfig build() {
            if (distributions == null || distributions.isEmpty())
                throw new IllegalStateException("At least one Distribution must be provided");
            return new GeneratorConfig(this);
        }
    }
}
