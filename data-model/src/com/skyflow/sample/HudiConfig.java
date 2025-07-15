package com.skyflow.sample;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface HudiConfig {

    String recordkey_field();
    String precombinekey_field();
    String partitionpathkey_field() default "";

}
