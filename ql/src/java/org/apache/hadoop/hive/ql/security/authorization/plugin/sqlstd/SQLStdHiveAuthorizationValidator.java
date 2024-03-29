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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationValidator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.Operation2Privilege.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SQLStdHiveAuthorizationValidator implements HiveAuthorizationValidator {

  private final HiveMetastoreClientFactory metastoreClientFactory;
  private final HiveConf conf;
  private final HiveAuthenticationProvider authenticator;
  private final SQLStdHiveAccessControllerWrapper privController;
  private final HiveAuthzSessionContext ctx;
  public static final Logger LOG = LoggerFactory.getLogger(SQLStdHiveAuthorizationValidator.class);

  public SQLStdHiveAuthorizationValidator(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider authenticator,
      SQLStdHiveAccessControllerWrapper privilegeManager, HiveAuthzSessionContext ctx)
      throws HiveAuthzPluginException {

    this.metastoreClientFactory = metastoreClientFactory;
    this.conf = conf;
    this.authenticator = authenticator;
    this.privController = privilegeManager;
    this.ctx = SQLAuthorizationUtils.applyTestSettings(ctx, conf);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {

    if (LOG.isDebugEnabled()) {
      String msg = "Checking privileges for operation " + hiveOpType + " by user "
          + authenticator.getUserName() + " on " + " input objects " + inputHObjs
          + " and output objects " + outputHObjs + ". Context Info: " + context;
      LOG.debug(msg);
    }

    // 实际调用的sessionState.getUserName()
    String userName = authenticator.getUserName();
    // 调用的是Hive.get().getMSC();来获取client
    IMetaStoreClient metastoreClient = metastoreClientFactory.getHiveMetastoreClient();

    // check privileges on input and output objects
    List<String> deniedMessages = new ArrayList<String>();
    // 检查输入输出的权限, 进入checkPrivileges方法
    checkPrivileges(hiveOpType, inputHObjs, metastoreClient, userName, IOType.INPUT, deniedMessages);
    // 检查输出的权限, 如果是查询的话不会有输出
    checkPrivileges(hiveOpType, outputHObjs, metastoreClient, userName, IOType.OUTPUT, deniedMessages);

    // 如果deniedMessages size不为0， 打印错误信息， 抛出HiveAccessControlException
    SQLAuthorizationUtils.assertNoDeniedPermissions(new HivePrincipal(userName,
        HivePrincipalType.USER), hiveOpType, deniedMessages);
  }

  private void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> hiveObjects,
      IMetaStoreClient metastoreClient, String userName, IOType ioType, List<String> deniedMessages)
      throws HiveAuthzPluginException, HiveAccessControlException {

    if (hiveObjects == null) {
      return;
    }

    // Compare required privileges and available privileges for each hive object
    for (HivePrivilegeObject hiveObj : hiveObjects) {

      // 获取执行该HQL需要的权限信息, 比如add jar, 返回的是封装了SQLPrivTypeGrant.ADMIN_PRIV的requiredPrivs
      RequiredPrivileges requiredPrivs = Operation2Privilege.getRequiredPrivs(hiveOpType, hiveObj,
          ioType);

      if(requiredPrivs.getRequiredPrivilegeSet().isEmpty()){
        // no privileges required, so don't need to check this object privileges
        continue;
      }

      // 根据HivePrivilegeObjectType类型来获取可用的权限, 通常是default
      // find available privileges
      RequiredPrivileges availPrivs = new RequiredPrivileges(); //start with an empty priv set;
      switch (hiveObj.getType()) {
      case LOCAL_URI:
      case DFS_URI:
        availPrivs = SQLAuthorizationUtils.getPrivilegesFromFS(new Path(hiveObj.getObjectName()),
            conf, userName);
        break;
      case PARTITION:
        // sql std authorization is managing privileges at the table/view levels
        // only
        // ignore partitions
        continue;
      case COMMAND_PARAMS:
      case FUNCTION:
        // operations that have objects of type COMMAND_PARAMS, FUNCTION are authorized
        // solely on the type
        if (privController.isUserAdmin()) {
          availPrivs.addPrivilege(SQLPrivTypeGrant.ADMIN_PRIV);
        }
        break;
      default:
        // 调用getPrivilegesFromMetaStore方法
        availPrivs = SQLAuthorizationUtils.getPrivilegesFromMetaStore(metastoreClient, userName,
            hiveObj, privController.getCurrentRoleNames(), privController.isUserAdmin());
      }

      // 检查availPrivs是否包含requiredPrivs, 如果包含missingPriv size为0, 否则missingPriv包含缺少的权限
      // Verify that there are no missing privileges
      Collection<SQLPrivTypeGrant> missingPriv = requiredPrivs.findMissingPrivs(availPrivs);
      // 将缺少的权限添加到deniedMessages中
      SQLAuthorizationUtils.addMissingPrivMsg(missingPriv, hiveObj, deniedMessages);

    }
  }

  @Override
  public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs,
      HiveAuthzContext context) {
    if (LOG.isDebugEnabled()) {
      String msg = "Obtained following objects in  filterListCmdObjects " + listObjs + " for user "
          + authenticator.getUserName() + ". Context Info: " + context;
      LOG.debug(msg);
    }
    return listObjs;
  }

  @Override
  public boolean needTransform() {
    return false;
  }

  @Override
  public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext context,
      List<HivePrivilegeObject> privObjs) throws SemanticException {
    return null;
  }

}
