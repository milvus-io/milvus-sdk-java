package com.zilliz.milvustest.util;

import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

public class MathUtil {
  public static String getRandomString(int length) {
    String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(62);
      sb.append(str.charAt(number));
    }
    return sb.toString();
  }

  public static boolean delAllFile(String path) {
    boolean flag = false;
    File file = new File(path);
    if (!file.exists()) {
      return flag;
    }
    if (!file.isDirectory()) {
      return flag;
    }
    String[] tempList = file.list();
    File temp = null;
    for (int i = 0; i < tempList.length; i++) {
      if (path.endsWith(File.separator)) {
        temp = new File(path + tempList[i]);
      } else {
        temp = new File(path + File.separator + tempList[i]);
      }
      if (temp.isFile()) {
        temp.delete();
      }
      if (temp.isDirectory()) {
        delAllFile(path + "/" + tempList[i]); // 先删除文件夹里面的文件
        flag = true;
      }
    }
    return flag;
  }

  public static Integer[] generateIntPK(int num, Boolean sequence, int fileNumber) {
    Integer[] intData = new Integer[num];
    Random random = new Random(num);
    if (sequence) {
      for (int i = 0; i < num; i++) {
        intData[i] = i + num * (fileNumber - 1);
      }
    }
    if (!sequence) {
      List<Integer> lists = new ArrayList<>();
      for (int i = 0; i < num; i++) {
        lists.add(i + num * (fileNumber - 1));
      }
      for (int i = 0; i < num; i++) {
        int index = random.nextInt(lists.size());
        intData[i] = lists.get(index);
        lists.remove(index);
      }
    }
    return intData;
  }

  public static Integer[] generateInt(int num, Boolean sequence) {
    Integer[] intData = new Integer[num];
    Random random = new Random(num);
    if (sequence) {
      for (int i = 0; i < num; i++) {
        intData[i] = i;
      }
    }
    if (!sequence) {
      List<Integer> lists = new ArrayList<>();
      for (int i = 0; i < num; i++) {
        lists.add(i);
      }
      for (int i = 0; i < num; i++) {
        int index = random.nextInt(lists.size());
        intData[i] = lists.get(index);
        lists.remove(index);
      }
    }
    return intData;
  }

  public static Float[] generateFloat(int num) {
    Float[] floats = new Float[num];
    Random random = new Random(num);
    for (int i = 0; i < num; i++) {
      floats[i] = random.nextFloat();
    }
    return floats;
  }

  public static String[] generateString(int num) {
    String[] strings = new String[num];
    for (int i = 0; i < num; i++) {
      strings[i] = getRandomString(15);
    }
    return strings;
  }

  public static List<Double[]> generateFloatVector(int num, int length, int dim) {
    List<Double[]> doubleList = new ArrayList<>(num);
    for (int j = 0; j < num; j++) {
      Double[] doubles = new Double[dim];
      for (int i = 0; i < dim; i++) {
        BigDecimal bigDecimal = BigDecimal.valueOf(Math.random());
        BigDecimal bigDecimal1 = bigDecimal.setScale(length, BigDecimal.ROUND_HALF_UP);
        doubles[i] = bigDecimal1.doubleValue();
      }
      doubleList.add(doubles);
    }
    return doubleList;
  }

  public static List<int[]> generateBinaryVectors(int num, int dim) {
    Random random=new Random();
    List<int[]> intList = new ArrayList<>(num);
    for (int j = 0; j < num; j++) {
      int[] intvalue = new int[dim/8];
      for (int i = 0; i < dim/8; i++) {

        intvalue[i] = random.nextInt(100);
      }
      intList.add(intvalue);
    }
    return intList;

  }

  public static Boolean[] generateBoolean(int num) {
    Boolean[] booleans = new Boolean[num];
    Random random = new Random();
    for (int i = 0; i < num; i++) {
      if (random.nextInt() % 2 == 0) {
        booleans[i] = Boolean.TRUE;
      } else {
        booleans[i] = Boolean.FALSE;
      }
    }
    return booleans;
  }

  public static Long[] generateLong(int num) {
    Long[] longs = new Long[num];
    Random random = new Random();
    for (int i = 0; i < num; i++) {
      longs[i] = random.nextLong();
    }
    return longs;
  }

  public static Object[][] combine(Object[][] a1, Object[][] a2) {
    List<Object[]> objectCodesList = new LinkedList<Object[]>();
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

  public static String genRandomStringAndChinese(int length){
    String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    String chinese="富强民主文明和谐自由平等公正法治爱国敬业诚信友善";
    String strChinese=str+chinese;
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(strChinese.length());
      sb.append(strChinese.charAt(number));
    }
    return sb.toString();
  }
}
