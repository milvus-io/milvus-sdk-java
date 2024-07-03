package com.zilliz.milvustestv2.utils;

import io.milvus.v2.common.IndexParam;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DataProviderUtils {
    public static Object[][] generateDataSets(Object[][]... arrays) {
        Object[][] testData=arrays[0];
        for (int i = 1; i < arrays.length; i++) {
            testData=combine(testData,arrays[i]);
        }
        return testData;
    }


    public static void main(String[] args) {
        Object[][] array1 = {{IndexParam.MetricType.L2,"a"}, {IndexParam.MetricType.IP,"b"}, {IndexParam.MetricType.HAMMING,"c"}};
        Object[][] array2 = {{"1"}, {"2"}, {"3"}};
        Object[][] array3 = {{"x"}, {"y"}, {"z"}};
        Object[][] array40 = {{110,1100}, {220,2200}, {330,3300}};
        Object[][] array4 = {{10,100}, {20,200}, {30,300}};
        String[] array5 = {"I", "II", "III"};

        Object[][] combine = generateDataSets(array1,array4);

        for (Object[] objects : combine) {
            for (Object object : objects) {
                System.out.print(object+" ");
            }
            System.out.println("----------");
        }
    }



    public static Object[][] combine(Object[][] a1, Object[][] a2) {
        List<Object[]> objectCodesList = new LinkedList<>();
        for (Object[] o : a1) {
            for (Object[] o2 : a2) {
                objectCodesList.add(concatAll(o, o2));
            }
        }
        return objectCodesList.toArray(new Object[0][0]);
    }

    public static <T> T[] concatAll(T[] first, T[]... rest) {
        // calculate the total length of the final object array after the concat
        int totalLength = first.length;
        for (T[] array : rest) {
            totalLength += array.length;
        }
        // copy the first array to result array and then copy each array completely to result
        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (T[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }

        return result;
    }
}
