package io.milvus.annotation;


import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RetryAnno {
    /**
     * 重试次数
     *
     * @return
     */
    int times() default 3;

    /**
     * 两次重试之间的间隔时间, 单位: ms
     * @return
     */
    long internal() default 100L;
}
