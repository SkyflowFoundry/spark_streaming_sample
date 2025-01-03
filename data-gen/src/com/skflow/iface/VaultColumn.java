package com.skflow.iface;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface VaultColumn {
    /**
     * The name of the column.
     */
    String value() default "";

    /**
     * The regular expression used by the vault to validate the field
     */
    String regex() default "";

    /**
     * Indicates if this column is the upsert key. Can only be true for one column in the object.
     */
    boolean upsertColumn() default false;

    /**
     * Indicates if the vault tokenizes the column when inserting / updating the object.
     */
    boolean tokenized() default true;

    /**
     * The format of the token if the column is tokenized.
     */
    String tokenFormat() default "";
}
