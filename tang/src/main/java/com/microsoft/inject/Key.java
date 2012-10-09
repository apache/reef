package com.microsoft.inject;

import java.lang.reflect.Field;

class Key {
    public final Class<?> keyClass;
    public final NamedParameter name;
    public final Field field;
    public Key(Class<?> c, Field f) {
        this.keyClass = c;
        this.field = f;
        this.name = null;
    }
    public Key(Class<?> c, NamedParameter name) {
        this.keyClass = c;
        this.field = null;
        this.name = name;
    }

}
