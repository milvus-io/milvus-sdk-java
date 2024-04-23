package com.zilliz.milvustest.query;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class MultipleArraysDataFactory {

    @DataProvider(name = "dataSets")
    public static Object[][] dataSets() {
        String[] array1 = {"A", "B", "C"};
        String[] array2 = {"1", "2", "3"};
        String[] array3 = {"X", "Y", "Z"};
        String[] array4 = {"10", "20", "30"};
        String[] array5 = {"I", "II", "III"};

        Object[][] testData = new Object[array1.length * array2.length * array3.length * array4.length * array5.length][5];
        int index = 0;
        for (String value1 : array1) {
            for (String value2 : array2) {
                for (String value3 : array3) {
                    for (String value4 : array4) {
                        for (String value5 : array5) {
                            testData[index][0] = value1;
                            testData[index][1] = value2;
                            testData[index][2] = value3;
                            testData[index][3] = value4;
                            testData[index][4] = value5;
                            index++;
                        }
                    }
                }
            }
        }
        return testData;
    }

    @Factory(dataProvider = "dataSets")
    public Object[] createInstances(String param1, String param2, String param3, String param4, String param5) {
        return new Object[] { new TestData(param1, param2, param3, param4, param5) };
    }

    public class TestData {
        private String param1;
        private String param2;
        private String param3;
        private String param4;
        private String param5;

        public TestData(String param1, String param2, String param3, String param4, String param5) {
            this.param1 = param1;
            this.param2 = param2;
            this.param3 = param3;
            this.param4 = param4;
            this.param5 = param5;
        }

        @Test
        public void testMultipleArraysData() {
            System.out.println("Parameters: " + param1 + ", " + param2 + ", " + param3 + ", " + param4 + ", " + param5);
            // 在这里执行测试逻辑
        }
    }
}
