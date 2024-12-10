/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.bulkwriter.common.utils;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ImportUtils {
    public static String getCommonPrefix(List<List<String>> batchFiles) {
        List<String> allFilePaths = batchFiles.stream().flatMap(Collection::stream).collect(Collectors.toList());
        return longestCommonPrefix(allFilePaths);
    }

    private static String longestCommonPrefix(List<String> allFilePaths) {
        if (allFilePaths.size() == 0) {
            return "";
        }
        String prefix = allFilePaths.get(0);
        int count = allFilePaths.size();
        for (int i = 1; i < count; i++) {
            prefix = longestCommonPrefix(prefix, allFilePaths.get(i));
            if (prefix.length() == 0) {
                break;
            }
        }
        return prefix;
    }

    private static String longestCommonPrefix(String str1, String str2) {
        int length = Math.min(str1.length(), str2.length());
        int index = 0;
        while (index < length && str1.charAt(index) == str2.charAt(index)) {
            index++;
        }
        return str1.substring(0, index);
    }
}
