/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import java.util.Map;

/**
 * The class to store query level info such as queryId. Multiple queries can run
 * in the same session, so SessionState is to hold common session related info, and
 * each QueryState is to hold query related info.
 * 存储查询级别信息（如queryId）的类。 多个查询可以在同一个会话中运行，因此SessionState将保存公共会话相关信息，每个QueryState都将保存查询相关信息。
 *
 */
public class QueryState {
  /**
   * current configuration.
   */
  private final HiveConf queryConf;
  /**
   * type of the command.
   */
  // 查询操作类型, 在SemanticAnalyzerFactory的get(QueryState queryState, ASTNode tree)方法中会设置该值, 该值是一个枚举类, 比如: HiveOperation.QUERY
  private HiveOperation commandType;

  public QueryState(HiveConf conf) {
    this(conf, null, false);
  }

  public QueryState(HiveConf conf, Map<String, String> confOverlay, boolean runAsync) {
    // 初始化queryConf, runAsync默认是false, 创建queryID
    this.queryConf = createConf(conf, confOverlay, runAsync);
  }

  /**
   * If there are query specific settings to overlay, then create a copy of config
   * There are two cases we need to clone the session config that's being passed to hive driver
   * 1. Async query -
   *    If the client changes a config setting, that shouldn't reflect in the execution already underway
   * 2. confOverlay -
   *    The query specific settings should only be applied to the query config and not session
   * @return new configuration
   */
  private HiveConf createConf(HiveConf conf,
      Map<String, String> confOverlay,
      boolean runAsync) {

    // 处理confOverlay
    if ( (confOverlay != null && !confOverlay.isEmpty()) ) {
      conf = (conf == null ? new HiveConf() : new HiveConf(conf));

      // apply overlay query specific settings, if any
      for (Map.Entry<String, String> confEntry : confOverlay.entrySet()) {
        try {
          conf.verifyAndSet(confEntry.getKey(), confEntry.getValue());
        } catch (IllegalArgumentException e) {
          throw new RuntimeException("Error applying statement specific settings", e);
        }
      }
    } else if (runAsync) {
      conf = (conf == null ? new HiveConf() : new HiveConf(conf));
    }

    if (conf == null) {
      conf = new HiveConf();
    }

    // 设置该条hql的queryId, 如: hadoop_20170813122140_c6a1eeb6-a9d1-4ee3-8b83-59a386d8fdfc
    conf.setVar(HiveConf.ConfVars.HIVEQUERYID, QueryPlan.makeQueryId());
    return conf;
  }

  public String getQueryId() {
    return (queryConf.getVar(HiveConf.ConfVars.HIVEQUERYID));
  }

  public String getQueryString() {
    return queryConf.getQueryString();
  }

  public String getCommandType() {
    if (commandType == null) {
      return null;
    }
    return commandType.getOperationName();
  }

  public HiveOperation getHiveOperation() {
    return commandType;
  }

  public void setCommandType(HiveOperation commandType) {
    this.commandType = commandType;
  }

  public HiveConf getConf() {
    return queryConf;
  }
}
