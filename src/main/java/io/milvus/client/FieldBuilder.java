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

package io.milvus.client;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;

/** Contains Field Parameter Builder */
public class FieldBuilder {
  private Map<String, Object> fields;

  /** Initialize required field information: field name and data type */
  public FieldBuilder(String fieldName, DataType fieldType) {
    this.fields = new HashMap<>();
    this.fields.put("field", fieldName);
    this.fields.put("type", fieldType);
  }

  /**
   * Add key-value pair to <code>fields</code>.
   *
   * @param key The param key
   * @param value The param value
   * @return <code>FieldBuilder</code>
   */
  public FieldBuilder param(String key, Object value) {
    if (!fields.containsKey("params")) {
      fields.put("params", new JSONObject().put(key, value));
    } else {
      ((JSONObject) fields.get("params")).put(key, value);
    }
    return this;
  }

  /**
   * Add values to fields. Used for insert operation. This should be a list of object whose type
   * corresponds to relative field DataType.
   *
   * @param value The value
   * @return <code>FieldBuilder</code>
   */
  public FieldBuilder values(Object value) {
    this.fields.put("values", value);
    return this;
  }

  public Map<String, Object> build() {
    return this.fields;
  }
}
