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

package org.apache.hadoop.hive.ql.processors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * AddResourceProcessor.
 *
 */
public class AddResourceProcessor implements CommandProcessor {

  public static final Logger LOG = LoggerFactory.getLogger(AddResourceProcessor.class
      .getName());
  public static final LogHelper console = new LogHelper(LOG);

  @Override
  public void init() {
  }

  // 处理add xxx命令的, 注意传入的command是已经截掉第一个单词后的命令, 比如 命令是: add jar /abc.jar, 传过来的cmmand是 jar /abc.jar
  @Override
  public CommandProcessorResponse run(String command) {
    SessionState ss = SessionState.get();
    // 变量替换, 使用SessionState的hiveVariables替换cmmand中的变量
    command = new VariableSubstitution(new HiveVariableSource() {
      @Override
      public Map<String, String> getHiveVariable() {
        return SessionState.get().getHiveVariables();
      }
    }).substitute(ss.getConf(),command);
    String[] tokens = command.split("\\s+");
    SessionState.ResourceType t;
    // 如果tokens长度小于2, 并且token[0]不是FILE,JAR,ARCHIVE, 打印错误信息, 返回
    if (tokens.length < 2
        || (t = SessionState.find_resource_type(tokens[0])) == null) {
      console.printError("Usage: add ["
          + StringUtils.join(SessionState.ResourceType.values(), "|")
          + "] <value> [<value>]*");
      return new CommandProcessorResponse(1);
    }

    CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommand(ss, HiveOperationType.ADD, Arrays.asList(tokens));
    if(authErrResp != null){
      // there was an authorization issue
      return authErrResp;
    }

    try {
      ss.add_resources(t,
          Arrays.asList(Arrays.copyOfRange(tokens, 1, tokens.length)));
    } catch (Exception e) {
      return CommandProcessorResponse.create(e);
    }
    return new CommandProcessorResponse(0);
  }

}
