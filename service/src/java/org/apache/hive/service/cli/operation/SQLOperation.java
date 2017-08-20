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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.CharEncoding;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.ThriftJDBCBinarySerDe;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.server.ThreadWithGarbageCleanup;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * SQLOperation.
 *
 */
@SuppressWarnings("deprecation")
public class SQLOperation extends ExecuteStatementOperation {
  private Driver driver = null;
  private CommandProcessorResponse response;
  private TableSchema resultSchema = null;
  private Schema mResultSchema = null;
  private SerDe serde = null;
  private boolean fetchStarted = false;
  private volatile MetricsScope currentSQLStateScope;
  // Display for WebUI.
  private SQLOperationDisplay sqlOpDisplay;
  private long queryTimeout;
  private ScheduledExecutorService timeoutExecutor;
  private final boolean runAsync;

  /**
   * A map to track query count running by each user
   */
  private static Map<String, AtomicInteger> userQueries = new HashMap<String, AtomicInteger>();
  private static final String ACTIVE_SQL_USER = MetricsConstant.SQL_OPERATION_PREFIX + "active_user";

  public SQLOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay,
      boolean runInBackground, long queryTimeout) {
    // TODO: call setRemoteUser in ExecuteStatementOperation or higher.
    // 调用父类ExecuteStatementOperation的构造方法
    super(parentSession, statement, confOverlay, runInBackground);
    // beeline调用的时候runAsync是true
    this.runAsync = runInBackground;
    // queryTimeout默认是0
    this.queryTimeout = queryTimeout;
    setupSessionIO(parentSession.getSessionState());
    try {
      sqlOpDisplay = new SQLOperationDisplay(this);
    } catch (HiveSQLException e) {
      LOG.warn("Error calcluating SQL Operation Display for webui", e);
    }
  }

  @Override
  public boolean shouldRunAsync() {
    return runAsync;
  }

  private void setupSessionIO(SessionState sessionState) {
    try {
      sessionState.in = null; // hive server's session input stream is not used
      sessionState.out = new PrintStream(System.out, true, CharEncoding.UTF_8);
      sessionState.info = new PrintStream(System.err, true, CharEncoding.UTF_8);
      sessionState.err = new PrintStream(System.err, true, CharEncoding.UTF_8);
    } catch (UnsupportedEncodingException e) {
        LOG.error("Error creating PrintStream", e);
        e.printStackTrace();
        sessionState.out = null;
        sessionState.info = null;
        sessionState.err = null;
      }
    }

  /**
   * Compile the query and extract metadata
   * @param queryState
   * @throws HiveSQLException
   */
  public void prepare(QueryState queryState) throws HiveSQLException {
    // 调用父类的setState方法设置状态
    setState(OperationState.RUNNING);
    try {
      // 实例化driver
      driver = new Driver(queryState, getParentSession().getUserName());

      // Start the timer thread for cancelling the query when query timeout is reached
      // queryTimeout == 0 means no timeout
      // 当达到查询超时时启动定时器线程取消查询, 如果queryTimeout为0, 则表示不存在超时
      LOG.info("-----******>>> queryTimeout:" + queryTimeout);
      if (queryTimeout > 0) {
        timeoutExecutor = new ScheduledThreadPoolExecutor(1);
        Runnable timeoutTask = new Runnable() {
          @Override
          public void run() {
            try {
              LOG.info("Query timed out after: " + queryTimeout
                  + " seconds. Cancelling the execution now.");
              SQLOperation.this.cancel(OperationState.TIMEDOUT);
            } catch (HiveSQLException e) {
              LOG.error("Error cancelling the query after timeout: " + queryTimeout + " seconds", e);
            } finally {
              // Stop
              timeoutExecutor.shutdown();
            }
          }
        };
        timeoutExecutor.schedule(timeoutTask, queryTimeout, TimeUnit.SECONDS);
      }

      sqlOpDisplay.setQueryDisplay(driver.getQueryDisplay());

      // set the operation handle information in Driver, so that thrift API users
      // can use the operation handle they receive, to lookup query information in
      // Yarn ATS
      // 设置OperationId, 比如: KtahFvbXQYGKzFUH40h5fA
      String guid64 = Base64.encodeBase64URLSafeString(getHandle().getHandleIdentifier()
          .toTHandleIdentifier().getGuid()).trim();
      LOG.info("++++++++>> operationId:" + guid64);
      driver.setOperationId(guid64);

      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      driver.setTryCount(Integer.MAX_VALUE);

      // 编译statement, 生成执行计划, 权限校验, 核心的地方
      response = driver.compileAndRespond(statement);
      if (0 != response.getResponseCode()) {
        throw toSQLException("Error while compiling statement", response);
      }

      // 上面在执行driver的compileAndRespond方法的过程中会设置schema
      mResultSchema = driver.getSchema();

      // hasResultSet should be true only if the query has a FetchTask
      // "explain" is an exception for now
      if(driver.getPlan().getFetchTask() != null) {
        LOG.info("+++++-----++++> getFetchTask() != null");
        //Schema has to be set
        if (mResultSchema == null || !mResultSchema.isSetFieldSchemas()) {
          throw new HiveSQLException("Error compiling query: Schema and FieldSchema " +
              "should be set when query plan has a FetchTask");
        }
        resultSchema = new TableSchema(mResultSchema);
        // 设置hasResultSet为true, 同时设置OperationHandle的hasResultSet为true, OperationHandle这个值是客户端用来判断是否有result的标识位
        setHasResultSet(true);
      } else {
        setHasResultSet(false);
      }
      // Set hasResultSet true if the plan has ExplainTask
      // TODO explain should use a FetchTask for reading
      // 判断是不是ExplainTask, 如果是的话将是否含有ResultSet设为true
      for (Task<? extends Serializable> task: driver.getPlan().getRootTasks()) {
        if (task.getClass() == ExplainTask.class) {
          LOG.info("------>>> mResultSchema:" + mResultSchema);
          resultSchema = new TableSchema(mResultSchema);
          setHasResultSet(true);
          break;
        }
      }
    } catch (HiveSQLException e) {
      setState(OperationState.ERROR);
      throw e;
    } catch (Throwable e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error running query: " + e.toString(), e);
    }
  }

  private void runQuery() throws HiveSQLException {
    try {
      OperationState opState = getStatus().getState();
      // Operation may have been cancelled by another thread
      if (opState.isTerminal()) {
        LOG.info("Not running the query. Operation is already in terminal state: " + opState
            + ", perhaps cancelled due to query timeout or by another thread.");
        return;
      }
      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      driver.setTryCount(Integer.MAX_VALUE);
      // 调用Driver的run方法来执行mapreduce Task
      response = driver.run();
      if (0 != response.getResponseCode()) {
        throw toSQLException("Error while processing statement", response);
      }
    } catch (HiveSQLException e) {
      /**
       * If the operation was cancelled by another thread, or the execution timed out, Driver#run
       * may return a non-zero response code. We will simply return if the operation state is
       * CANCELED, TIMEDOUT or CLOSED, otherwise throw an exception
       */
      if ((getStatus().getState() == OperationState.CANCELED)
          || (getStatus().getState() == OperationState.TIMEDOUT)
          || (getStatus().getState() == OperationState.CLOSED)) {
        return;
      } else {
        setState(OperationState.ERROR);
        throw e;
      }
    } catch (Throwable e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error running query: " + e.toString(), e);
    }
    // 更新状态为finished
    setState(OperationState.FINISHED);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    // 调用父类方法更新状态
    setState(OperationState.PENDING);

    // runAsync通过beeline调用的话是true
    boolean runAsync = shouldRunAsync();
    // runAsync按true处理, hive.server2.async.exec.async.compile默认是false, 所以asyncPrepare是false
    final boolean asyncPrepare = runAsync
      && HiveConf.getBoolVar(queryState.getConf(),
        HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_ASYNC_COMPILE);
    LOG.info("++++>>>+++>>> runAsync:" + runAsync + ", asyncPrePare:" + asyncPrepare);
    // asyncPrepare是false, 所以执行prepare方法, 会对hql进行编译生成执行计划
    if (!asyncPrepare) {
      prepare(queryState);
    }
    LOG.info("+++++++ prepare finished!");
    // runAsync是true执行else中的逻辑, 即异步执行查询计划
    if (!runAsync) {
      runQuery();
    } else {
      // We'll pass ThreadLocals in the background thread from the foreground (handler) thread.
      // 1) ThreadLocal Hive object needs to be set in background thread
      // 2) The metastore client in Hive is associated with right user.
      // 3) Current UGI will get used by metastore when metastore is in embedded mode
      Runnable work = new BackgroundWork(getCurrentUGI(), parentSession.getSessionHive(),
          SessionState.getPerfLogger(), SessionState.get(), asyncPrepare);

      try {
        // This submit blocks if no background threads are available to run this operation
        // 调用HiveSessionImpl的submitBackgroundOperation方法
        Future<?> backgroundHandle = getParentSession().submitBackgroundOperation(work);
        // 调用setBackgroundHandle方法将上面的backgroundHandle设置给SQLOperation的backgroundHandle
        setBackgroundHandle(backgroundHandle);
      } catch (RejectedExecutionException rejected) {
        setState(OperationState.ERROR);
        throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected);
      }
    }
  }


  private final class BackgroundWork implements Runnable {
    private final UserGroupInformation currentUGI;
    private final Hive parentHive;
    private final PerfLogger parentPerfLogger;
    private final SessionState parentSessionState;
    private final boolean asyncPrepare;

    private BackgroundWork(UserGroupInformation currentUGI,
        Hive parentHive, PerfLogger parentPerfLogger,
        SessionState parentSessionState, boolean asyncPrepare) {
      this.currentUGI = currentUGI;
      this.parentHive = parentHive;
      this.parentPerfLogger = parentPerfLogger;
      this.parentSessionState = parentSessionState;
      this.asyncPrepare = asyncPrepare;
    }

    @Override
    public void run() {
      PrivilegedExceptionAction<Object> doAsAction = new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws HiveSQLException {
          Hive.set(parentHive);
          // TODO: can this result in cross-thread reuse of session state?
          SessionState.setCurrentSessionState(parentSessionState);
          PerfLogger.setPerfLogger(parentPerfLogger);
          // Set current OperationLog in this async thread for keeping on saving query log.
          // 将SQLOperation的OperationLog设置给当前线程
          registerCurrentOperationLog();
          // 注册当前线程的Context
          registerLoggingContext();
          try {
            // 通过beeline调用时, asyncPrepare是false
            if (asyncPrepare) {
              prepare(queryState);
            }
            // 最核心的地方, 调用SQLOperation的runQuery方法
            runQuery();
          } catch (HiveSQLException e) {
            // 调用父类方法设置异常信息
            setOperationException(e);
            LOG.error("Error running hive query: ", e);
          } finally {
            // 移除设置的Context信息
            unregisterLoggingContext();
            // 移除注册在当前线程的OperationLog
            unregisterOperationLog();
          }
          return null;
        }
      };

      try {
        currentUGI.doAs(doAsAction);
      } catch (Exception e) {
        // 调用父类的方法设置异常信息
        setOperationException(new HiveSQLException(e));
        LOG.error("Error running hive query as user : " + currentUGI.getShortUserName(), e);
      }
      finally {
        /**
         * We'll cache the ThreadLocal RawStore object for this background thread for an orderly cleanup
         * when this thread is garbage collected later.
         * @see org.apache.hive.service.server.ThreadWithGarbageCleanup#finalize()
         */
        if (ThreadWithGarbageCleanup.currentThread() instanceof ThreadWithGarbageCleanup) {
          ThreadWithGarbageCleanup currentThread =
              (ThreadWithGarbageCleanup) ThreadWithGarbageCleanup.currentThread();
          currentThread.cacheThreadLocalRawStore();
        }
      }
    }
  }


  /**
   * Returns the current UGI on the stack
   * @param opConfig
   * @return UserGroupInformation
   * @throws HiveSQLException
   */
  private UserGroupInformation getCurrentUGI() throws HiveSQLException {
    try {
      return Utils.getUGI();
    } catch (Exception e) {
      throw new HiveSQLException("Unable to get current user", e);
    }
  }

  private void registerCurrentOperationLog() {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        LOG.warn("Failed to get current OperationLog object of Operation: " +
            getHandle().getHandleIdentifier());
        isOperationLogEnabled = false;
        return;
      }
      OperationLog.setCurrentOperationLog(operationLog);
    }
  }

  private synchronized void cleanup(OperationState state) throws HiveSQLException {
    setState(state);

    if (shouldRunAsync()) {
      Future<?> backgroundHandle = getBackgroundHandle();
      if (backgroundHandle != null) {
        boolean success = backgroundHandle.cancel(true);
        if (success) {
          LOG.info("The running operation has been successfully interrupted.");
        }
      }
    }

    if (driver != null) {
      driver.close();
      driver.destroy();
    }
    driver = null;

    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.warn("Operation seems to be in invalid state, SessionState is null");
    } else {
      ss.deleteTmpOutputFile();
      ss.deleteTmpErrOutputFile();
    }

    // Shutdown the timeout thread if any, while closing this operation
    if ((timeoutExecutor != null) && (state != OperationState.TIMEDOUT) && (state.isTerminal())) {
      timeoutExecutor.shutdownNow();
    }
  }

  @Override
  public void cancel(OperationState stateAfterCancel) throws HiveSQLException {
    cleanup(stateAfterCancel);
    cleanupOperationLog();
  }

  @Override
  public void close() throws HiveSQLException {
    cleanup(OperationState.CLOSED);
    cleanupOperationLog();
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    // Since compilation is always a blocking RPC call, and schema is ready after compilation,
    // we can return when are in the RUNNING state.
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.RUNNING,
        OperationState.FINISHED)));
    if (resultSchema == null) {
      resultSchema = new TableSchema(driver.getSchema());
    }
    return resultSchema;
  }

  private transient final List<Object> convey = new ArrayList<Object>();

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows)
    throws HiveSQLException {

    validateDefaultFetchOrientation(orientation);
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));

    FetchTask fetchTask = driver.getFetchTask();
    boolean isBlobBased = false;

    if (fetchTask != null && fetchTask.getWork().isUsingThriftJDBCBinarySerDe()) {
      // Just fetch one blob if we've serialized thrift objects in final tasks
      maxRows = 1;
      isBlobBased = true;
    }
    driver.setMaxRows((int) maxRows);
    RowSet rowSet = RowSetFactory.create(resultSchema, getProtocolVersion(), isBlobBased);
    try {
      /* if client is requesting fetch-from-start and its not the first time reading from this operation
       * then reset the fetch position to beginning
       */
      if (orientation.equals(FetchOrientation.FETCH_FIRST) && fetchStarted) {
        driver.resetFetch();
      }
      fetchStarted = true;
      driver.setMaxRows((int) maxRows);
      if (driver.getResults(convey)) {
        return decode(convey, rowSet);
      }
      return rowSet;
    } catch (IOException e) {
      throw new HiveSQLException(e);
    } catch (CommandNeedRetryException e) {
      throw new HiveSQLException(e);
    } catch (Exception e) {
      throw new HiveSQLException(e);
    } finally {
      convey.clear();
    }
  }

  @Override
  public String getTaskStatus() throws HiveSQLException {
    if (driver != null) {
      List<QueryDisplay.TaskDisplay> statuses = driver.getQueryDisplay().getTaskDisplays();
      if (statuses != null) {
        ByteArrayOutputStream out = null;
        try {
          ObjectMapper mapper = new ObjectMapper();
          out = new ByteArrayOutputStream();
          mapper.writeValue(out, statuses);
          return out.toString("UTF-8");
        } catch (JsonGenerationException e) {
          throw new HiveSQLException(e);
        } catch (JsonMappingException e) {
          throw new HiveSQLException(e);
        } catch (IOException e) {
          throw new HiveSQLException(e);
        } finally {
          if (out != null) {
            try {
              out.close();
            } catch (IOException e) {
              throw new HiveSQLException(e);
            }
          }
        }
      }
    }
    // Driver not initialized
    return null;
  }

  private RowSet decode(List<Object> rows, RowSet rowSet) throws Exception {
    if (driver.isFetchingTable()) {
      return prepareFromRow(rows, rowSet);
    }
    return decodeFromString(rows, rowSet);
  }

  // already encoded to thrift-able object in ThriftFormatter
  private RowSet prepareFromRow(List<Object> rows, RowSet rowSet) throws Exception {
    for (Object row : rows) {
      rowSet.addRow((Object[]) row);
    }
    return rowSet;
  }

  private RowSet decodeFromString(List<Object> rows, RowSet rowSet)
      throws SQLException, SerDeException {
    getSerDe();
    StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object[] deserializedFields = new Object[fieldRefs.size()];
    Object rowObj;
    ObjectInspector fieldOI;

    int protocol = getProtocolVersion().getValue();
    for (Object rowString : rows) {
      try {
        rowObj = serde.deserialize(new BytesWritable(((String)rowString).getBytes("UTF-8")));
      } catch (UnsupportedEncodingException e) {
        throw new SerDeException(e);
      }
      for (int i = 0; i < fieldRefs.size(); i++) {
        StructField fieldRef = fieldRefs.get(i);
        fieldOI = fieldRef.getFieldObjectInspector();
        Object fieldData = soi.getStructFieldData(rowObj, fieldRef);
        deserializedFields[i] = SerDeUtils.toThriftPayload(fieldData, fieldOI, protocol);
      }
      rowSet.addRow(deserializedFields);
    }
    return rowSet;
  }

  private SerDe getSerDe() throws SQLException {
    if (serde != null) {
      return serde;
    }
    try {
      List<FieldSchema> fieldSchemas = mResultSchema.getFieldSchemas();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if (fieldSchemas != null && !fieldSchemas.isEmpty()) {
        for (int pos = 0; pos < fieldSchemas.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
          namesSb.append(fieldSchemas.get(pos).getName());
          typesSb.append(fieldSchemas.get(pos).getType());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        LOG.debug("Column names: " + names);
        props.setProperty(serdeConstants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        LOG.debug("Column types: " + types);
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      SerDeUtils.initializeSerDe(serde, new HiveConf(), props, null);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
    return serde;
  }

  /**
   * Get summary information of this SQLOperation for display in WebUI.
   */
  public SQLOperationDisplay getSQLOperationDisplay() {
    return sqlOpDisplay;
  }

  @Override
  protected void onNewState(OperationState state, OperationState prevState) {
    super.onNewState(state, prevState);
    currentSQLStateScope = setMetrics(currentSQLStateScope, MetricsConstant.SQL_OPERATION_PREFIX,
      MetricsConstant.COMPLETED_SQL_OPERATION_PREFIX, state);

    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      try {
        // New state is changed to running from something else (user is active)
        if (state == OperationState.RUNNING && prevState != state) {
          incrementUserQueries(metrics);
        }
        // New state is not running (user not active) any more
        if (prevState == OperationState.RUNNING && prevState != state) {
          decrementUserQueries(metrics);
        }
      } catch (IOException e) {
        LOG.warn("Error metrics", e);
      }
    }

    if (state == OperationState.FINISHED || state == OperationState.CANCELED || state == OperationState.ERROR) {
      //update runtime
      sqlOpDisplay.setRuntime(getOperationComplete() - getOperationStart());
    }

    if (state == OperationState.CLOSED) {
      sqlOpDisplay.closed();
    } else {
      //CLOSED state not interesting, state before (FINISHED, ERROR) is.
      sqlOpDisplay.updateState(state);
    }
  }

  private void incrementUserQueries(Metrics metrics) throws IOException {
    String username = parentSession.getUserName();
    if (username != null) {
      synchronized (userQueries) {
        AtomicInteger count = userQueries.get(username);
        if (count == null) {
          count = new AtomicInteger(0);
          AtomicInteger prev = userQueries.put(username, count);
          if (prev == null) {
            metrics.incrementCounter(ACTIVE_SQL_USER);
          } else {
            count = prev;
          }
        }
        count.incrementAndGet();
      }
    }
  }

  private void decrementUserQueries(Metrics metrics) throws IOException {
    String username = parentSession.getUserName();
    if (username != null) {
      synchronized (userQueries) {
        AtomicInteger count = userQueries.get(username);
        if (count != null && count.decrementAndGet() <= 0) {
          metrics.decrementCounter(ACTIVE_SQL_USER);
          userQueries.remove(username);
        }
      }
    }
  }

  public String getExecutionEngine() {
    return queryState.getConf().getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
  }

}
