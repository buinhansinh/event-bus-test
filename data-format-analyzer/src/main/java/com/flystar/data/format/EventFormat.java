package com.flystar.data.format;

import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by zack on 6/5/2016.
 */
public class EventFormat {

    private final String type;
    private final Set<String> fields;

    public EventFormat(String type, Set<String> fields) {
        this.type = type;
        this.fields = ImmutableSet.copyOf(new TreeSet<>(fields));

    }

    public String getType() {
        return type;
    }

    public Set<String> getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventFormat that = (EventFormat) o;

        if (!type.equals(that.type)) return false;
        return fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + fields.hashCode();
        return result;
    }
}
