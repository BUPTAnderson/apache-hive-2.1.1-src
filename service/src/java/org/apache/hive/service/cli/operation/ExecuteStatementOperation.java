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
package org.apache.hive.service.cli.operation;

import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.session.HiveSession;

import java.sql.SQLException;
import java.util.Map;

public abstract class ExecuteStatementOperation extends Operation {
  protected String statement = null;

  public ExecuteStatementOperation(HiveSession parentSession, String statement,
      Map<String, String> confOverlay, boolean runInBackground) {
    // 调用父类Operation的构造方法
    super(parentSession, confOverlay, OperationType.EXECUTE_STATEMENT);
    // statement是要执行的hql
    this.statement = statement;
  }

  public String getStatement() {
    return statement;
  }

  public static ExecuteStatementOperation newExecuteStatementOperation(HiveSession parentSession,
      String statement, Map<String, String> confOverlay, boolean runAsync, long queryTimeout)
      throws HiveSQLException {
    String[] tokens = statement.trim().split("\\s+");
    CommandProcessor processor = null;
    try {
      // 只有set, reset, dfs, add, list, reload, delete, compile开头的hql, 并且不是set role, delete from, reload function, set autocommit才会返回非null
      processor = CommandProcessorFactory.getForHiveCommand(tokens, parentSession.getHiveConf());
    } catch (SQLException e) {
      throw new HiveSQLException(e.getMessage(), e.getSQLState(), e);
    }
    if (processor == null) {
      // runAsync, queryTimeout makes sense only for a SQLOperation
      // 所以正常的hql查询, 返回的是SQLOperation, SQLOperation的构造方法中会调用父类的构造方法来构造OperationHandle对象
      // beeline调用的时候runAsync是true, 即异步执行
      return new SQLOperation(parentSession, statement, confOverlay, runAsync, queryTimeout);
    }
    return new HiveCommandOperation(parentSession, statement, processor, confOverlay);
  }
}
