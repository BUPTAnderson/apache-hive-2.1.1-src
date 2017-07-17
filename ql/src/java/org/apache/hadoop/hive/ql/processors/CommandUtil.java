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

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

class CommandUtil {
  public static final Logger LOG = LoggerFactory.getLogger(CommandUtil.class);

  /**
   * 判断给定类型和参数的命令的权限
   * Authorize command of given type and arguments
   *
   * @param ss
   * @param type
   * @param command
   * @return null if there was no authorization error. Otherwise returns  CommandProcessorResponse
   * capturing the authorization error
   * 如果没有授权错误则为null。 否则返回CommandProcessorResponse捕获的授权错误
   */
  static CommandProcessorResponse authorizeCommand(SessionState ss, HiveOperationType type,
      List<String> command) {
    if (ss == null) {
      // ss can be null in unit tests
      return null;
    }

    // 判断AuthorizationMode是不是V2,  是否开启了权限验证(hive.security.authorization.enabled参数设为true则开启, 默认为false)
    // 通过if语句可知, 只有开启权限验证(hive.security.authorization.enabled参数设为true)且AuthorizationMode为V2才会进行权限验证
    if (ss.isAuthorizationModeV2() &&
        HiveConf.getBoolVar(ss.getConf(), HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      String errMsg = "Error authorizing command " + command;
      try {
        // 判断有没有执行该HQL的权限
        authorizeCommandThrowEx(ss, type, command);
        // authorized to perform action
        return null;
      } catch (HiveAuthzPluginException e) {
        LOG.error(errMsg, e);
        return CommandProcessorResponse.create(e);
      } catch (HiveAccessControlException e) {
        LOG.error(errMsg, e);
        return CommandProcessorResponse.create(e);
      }
    }
    return null;
  }
  /**
   * Authorize command. Throws exception if the check fails
   * @param ss
   * @param type
   * @param command
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  static void authorizeCommandThrowEx(SessionState ss, HiveOperationType type,
      List<String> command) throws HiveAuthzPluginException, HiveAccessControlException {
    // 两个参数HivePrivilegeObjectType.COMMAND_PARAMS和HivePrivObjectActionType.OTHER
    HivePrivilegeObject commandObj = HivePrivilegeObject.createHivePrivilegeObject(command);
    HiveAuthzContext.Builder ctxBuilder = new HiveAuthzContext.Builder();
    ctxBuilder.setCommandString(Joiner.on(' ').join(command));
    ctxBuilder.setUserIpAddress(ss.getUserIpAddress());
    ctxBuilder.setForwardedAddresses(ss.getForwardedAddresses());
    // 这里我们查看SQLStdHiveAuthorizationValidator的checkPrivileges
    ss.getAuthorizerV2().checkPrivileges(type, Arrays.asList(commandObj), null, ctxBuilder.build());
  }


}
