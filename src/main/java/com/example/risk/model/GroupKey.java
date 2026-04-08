package com.example.risk.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable key used to identify aggregation groups.
 * Wraps a list of attribute values with correct equals/hashCode for Map use.
 */
public final class GroupKey {

    private final List<String> values;

    public GroupKey(List<String> values) {
        this.values = Collections.unmodifiableList(values);
    }

    /** Return a new GroupKey with the last element removed (for rollup). */
    public GroupKey withoutLast() {
        if (values.isEmpty()) throw new IllegalStateException("Already at root");
        return new GroupKey(values.subList(0, values.size() - 1));
    }

    public List<String> getValues()  { return values; }
    public boolean      isEmpty()    { return values.isEmpty(); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupKey)) return false;
        return Objects.equals(values, ((GroupKey) o).values);
    }

    @Override
    public int hashCode() { return Objects.hash(values); }

    @Override
    public String toString() { return values.toString(); }
}
