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

package org.apache.hadoop.hive.cli;

import com.google.common.base.Splitter;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.ArgumentCompleter.AbstractArgumentDelimiter;
import jline.console.completer.ArgumentCompleter.ArgumentDelimiter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.PersistentHistory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveInterruptUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.common.cli.ShellCmdExecutor;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.io.FetchConverter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.Validator;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper;
import org.apache.hadoop.hive.ql.exec.tez.TezJobExecHelper;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.util.StringUtils.stringifyException;


/**
 * CliDriver.
 *
 */
public class CliDriver {

  public static String prompt = null;
  public static String prompt2 = null; // when ';' is not yet seen
  public static final int LINES_TO_FETCH = 40; // number of lines to fetch in batch from remote hive server
  public static final int DELIMITED_CANDIDATE_THRESHOLD = 10;

  public static final String HIVERCFILE = ".hiverc";

  private final LogHelper console;
  protected ConsoleReader reader;
  private Configuration conf;

  public CliDriver() {
    // SessionState的get方法会获取当前线程的SessionStates对象, 没有的话new一个, 然后获取SessionStates的SessionState属性state, 这里state是null值
    SessionState ss = SessionState.get();
    conf = (ss != null) ? ss.getConf() : new Configuration();
    Logger LOG = LoggerFactory.getLogger("CliDriver");
    if (LOG.isDebugEnabled()) {
      LOG.debug("CliDriver inited with classpath {}", System.getProperty("java.class.path"));
    }
    console = new LogHelper(LOG);
  }

  public int processCmd(String cmd) {
    CliSessionState ss = (CliSessionState) SessionState.get();
    ss.setLastCommand(cmd);

    ss.updateThreadName();

    // Flush the print stream, so it doesn't include output from the last command
    ss.err.flush();
    String cmd_trimmed = cmd.trim();
    // 通过空格分割cmd_trimmed
    String[] tokens = tokenizeCmd(cmd_trimmed);
    int ret = 0;

    // 1. 如果是quit/exit, 退出
    if (cmd_trimmed.toLowerCase().equals("quit") || cmd_trimmed.toLowerCase().equals("exit")) {

      // if we have come this far - either the previous commands
      // are all successful or this is command line. in either case
      // this counts as a successful run
      ss.close();
      System.exit(0);

    // 2. 如果是 source (如: source /home/wyp/Documents/test; 执行文件中的sql, 类似于: bin/hive -f /home/wyp/Documents/test)
    } else if (tokens[0].equalsIgnoreCase("source")) {
      String cmd_1 = getFirstCmd(cmd_trimmed, tokens[0].length());
      // 用SessionState的hiveVariables对cmd_1中的变量${varname}进行替换
      cmd_1 = new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
          return SessionState.get().getHiveVariables();
        }
      }).substitute(ss.getConf(), cmd_1);

      File sourceFile = new File(cmd_1);
      if (! sourceFile.isFile()){
        console.printError("File: "+ cmd_1 + " is not a file.");
        ret = 1;
      } else {
        try {
          ret = processFile(cmd_1);
        } catch (IOException e) {
          console.printError("Failed processing file "+ cmd_1 +" "+ e.getLocalizedMessage(),
            stringifyException(e));
          ret = 1;
        }
      }
      // 3. 如果是!开头, 即以本地shell来执行该语句(比如, !pwd)
    } else if (cmd_trimmed.startsWith("!")) {

      String shell_cmd = cmd_trimmed.substring(1);
      // 用SessionState的hiveVariables对shell_cmd中的变量${varname}进行替换
      shell_cmd = new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
          return SessionState.get().getHiveVariables();
        }
      }).substitute(ss.getConf(), shell_cmd);

      // shell_cmd = "/bin/bash -c \'" + shell_cmd + "\'";
      try {
        ShellCmdExecutor executor = new ShellCmdExecutor(shell_cmd, ss.out, ss.err);
        ret = executor.execute();
        if (ret != 0) {
          console.printError("Command failed with exit code = " + ret);
        }
      } catch (Exception e) {
        console.printError("Exception raised from Shell command " + e.getLocalizedMessage(),
            stringifyException(e));
        ret = 1;
      }
      // 4. 其它的调用processLocalCmd进行处理
    }  else { // local mode
      try {
        // CommandProcessorFactory工厂解析输入的语句来获得一个CommandProcessor, 一般查询返回的是Drive, 注意每个查询都会创建一个CommandProcessor
        CommandProcessor proc = CommandProcessorFactory.get(tokens, (HiveConf) conf);
        // 使用CommandProcessor处理
        ret = processLocalCmd(cmd, proc, ss);
      } catch (SQLException e) {
        console.printError("Failed processing command " + tokens[0] + " " + e.getLocalizedMessage(),
          org.apache.hadoop.util.StringUtils.stringifyException(e));
        ret = 1;
      }
    }

    ss.resetThreadName();
    return ret;
  }

  /**
   * For testing purposes to inject Configuration dependency
   * @param conf to replace default
   */
  void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Extract and clean up the first command in the input.
   */
  private String getFirstCmd(String cmd, int length) {
    return cmd.substring(length).trim();
  }

  private String[] tokenizeCmd(String cmd) {
    return cmd.split("\\s+");
  }

  int processLocalCmd(String cmd, CommandProcessor proc, CliSessionState ss) {
    int tryCount = 0;
    boolean needRetry;
    int ret = 0;

    do {
      try {
        needRetry = false;
        if (proc != null) {
          // 如果是执行正常的SQL, proc是Driver
          if (proc instanceof Driver) {
            Driver qp = (Driver) proc;
            PrintStream out = ss.out;
            long start = System.currentTimeMillis();
            if (ss.getIsVerbose()) {
              out.println(cmd);
            }

            qp.setTryCount(tryCount);
            // 调用Driver的run方法
            ret = qp.run(cmd).getResponseCode();
            if (ret != 0) {
              qp.close();
              return ret;
            }

            // query has run capture the time
            long end = System.currentTimeMillis();
            double timeTaken = (end - start) / 1000.0;

            ArrayList<String> res = new ArrayList<String>();

            printHeader(qp, out);

            // print the results
            int counter = 0;
            try {
              if (out instanceof FetchConverter) {
                ((FetchConverter)out).fetchStarted();
              }
              while (qp.getResults(res)) {
                for (String r : res) {
                  out.println(r);
                }
                counter += res.size();
                res.clear();
                if (out.checkError()) {
                  break;
                }
              }
            } catch (IOException e) {
              console.printError("Failed with exception " + e.getClass().getName() + ":"
                  + e.getMessage(), "\n"
                  + org.apache.hadoop.util.StringUtils.stringifyException(e));
              ret = 1;
            }

            int cret = qp.close();
            if (ret == 0) {
              ret = cret;
            }

            if (out instanceof FetchConverter) {
              ((FetchConverter)out).fetchFinished();
            }

            console.printInfo("Time taken: " + timeTaken + " seconds" +
                (counter == 0 ? "" : ", Fetched: " + counter + " row(s)"));
          } else {
            String firstToken = tokenizeCmd(cmd.trim())[0];
            // cmd_1是已经除掉第一个token的语句, 比如 cmd是: add jar /abc.jar, cmd_1是 jar /abc.jar
            String cmd_1 = getFirstCmd(cmd.trim(), firstToken.length());

            if (ss.getIsVerbose()) {
              ss.out.println(firstToken + " " + cmd_1);
            }
            // 调用proc的run方法来处理cmd_1, 比如AddResourceProcessor, DeleteResourceProcessor, DfsProcessor, SetProcessor等
            CommandProcessorResponse res = proc.run(cmd_1);
            if (res.getResponseCode() != 0) {
              ss.out.println("Query returned non-zero code: " + res.getResponseCode() +
                  ", cause: " + res.getErrorMessage());
            }
            if (res.getConsoleMessages() != null) {
              for (String consoleMsg : res.getConsoleMessages()) {
                console.printInfo(consoleMsg);
              }
            }
            ret = res.getResponseCode();
          }
        }
      } catch (CommandNeedRetryException e) {
        console.printInfo("Retry query with a different approach...");
        tryCount++;
        needRetry = true;
      }
    } while (needRetry);

    return ret;
  }

  /**
   * If enabled and applicable to this command, print the field headers
   * for the output.
   *
   * @param qp Driver that executed the command
   * @param out PrintStream which to send output to
   */
  private void printHeader(Driver qp, PrintStream out) {
    List<FieldSchema> fieldSchemas = qp.getSchema().getFieldSchemas();
    // hive.cli.print.header默认值为false
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)
          && fieldSchemas != null) {
      // Print the column names
      boolean first_col = true;
      for (FieldSchema fs : fieldSchemas) {
        if (!first_col) {
          out.print('\t');
        }
        out.print(fs.getName());
        first_col = false;
      }
      out.println();
    }
  }

  public int processLine(String line) {
    return processLine(line, false);
  }

  /**
   * 处理一行分号分割的命令
   * Processes a line of semicolon separated commands
   *
   * @param line
   *          The commands to process
   * @param allowInterrupting
   *          When true the function will handle SIG_INT (Ctrl+C) by interrupting the processing and
   *          returning -1
   * @return 0 if ok
   */
  public int processLine(String line, boolean allowInterrupting) {
    SignalHandler oldSignal = null;
    Signal interruptSignal = null;

    if (allowInterrupting) {
      // Remember all threads that were running at the time we started line processing.
      // Hook up the custom Ctrl+C handler while processing this line
      interruptSignal = new Signal("INT");
      oldSignal = Signal.handle(interruptSignal, new SignalHandler() {
        private boolean interruptRequested;

        @Override
        public void handle(Signal signal) {
          boolean initialRequest = !interruptRequested;
          interruptRequested = true;

          // Kill the VM on second ctrl+c
          if (!initialRequest) {
            console.printInfo("Exiting the JVM");
            System.exit(127);
          }

          // Interrupt the CLI thread to stop the current statement and return
          // to prompt
          console.printInfo("Interrupting... Be patient, this might take some time.");
          console.printInfo("Press Ctrl+C again to kill JVM");

          // First, kill any running MR jobs
          HadoopJobExecHelper.killRunningJobs();
          TezJobExecHelper.killRunningJobs();
          HiveInterruptUtils.interrupt();
        }
      });
    }

    try {
      int lastRet = 0, ret = 0;

      String command = "";
      for (String oneCmd : line.split(";")) {

        if (StringUtils.endsWith(oneCmd, "\\")) {
          command += StringUtils.chop(oneCmd) + ";";
          continue;
        } else {
          command += oneCmd;
        }
        if (StringUtils.isBlank(command)) {
          continue;
        }

        // 调用processCmd来处理输入, 这里的command是一条hql且已经不带';'
        ret = processCmd(command);
        command = "";
        lastRet = ret;
        // 是否忽略error, 默认值false
        boolean ignoreErrors = HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIIGNOREERRORS);
        if (ret != 0 && !ignoreErrors) {
          CommandProcessorFactory.clean((HiveConf) conf);
          return ret;
        }
      }
      CommandProcessorFactory.clean((HiveConf) conf);
      return lastRet;
    } finally {
      // Once we are done processing the line, restore the old handler
      if (oldSignal != null && interruptSignal != null) {
        Signal.handle(interruptSignal, oldSignal);
      }
    }
  }

  public int processReader(BufferedReader r) throws IOException {
    String line;
    StringBuilder qsb = new StringBuilder();

    while ((line = r.readLine()) != null) {
      // Skipping through comments
      if (! line.startsWith("--")) {
        qsb.append(line + "\n");
      }
    }

    return (processLine(qsb.toString()));
  }

  public int processFile(String fileName) throws IOException {
    Path path = new Path(fileName);
    FileSystem fs;
    if (!path.toUri().isAbsolute()) {
      fs = FileSystem.getLocal(conf);
      path = fs.makeQualified(path);
    } else {
      fs = FileSystem.get(path.toUri(), conf);
    }
    BufferedReader bufferReader = null;
    int rc = 0;
    try {
      bufferReader = new BufferedReader(new InputStreamReader(fs.open(path)));
      rc = processReader(bufferReader);
    } finally {
      IOUtils.closeStream(bufferReader);
    }
    return rc;
  }

  public void processInitFiles(CliSessionState ss) throws IOException {
    boolean saveSilent = ss.getIsSilent();
    ss.setIsSilent(true);
    for (String initFile : ss.initFiles) {
      int rc = processFile(initFile);
      if (rc != 0) {
        System.exit(rc);
      }
    }
    if (ss.initFiles.size() == 0) {
      if (System.getenv("HIVE_HOME") != null) {
        String hivercDefault = System.getenv("HIVE_HOME") + File.separator +
          "bin" + File.separator + HIVERCFILE;
        if (new File(hivercDefault).exists()) {
          int rc = processFile(hivercDefault);
          if (rc != 0) {
            System.exit(rc);
          }
          console.printError("Putting the global hiverc in " +
                             "$HIVE_HOME/bin/.hiverc is deprecated. Please "+
                             "use $HIVE_CONF_DIR/.hiverc instead.");
        }
      }
      if (System.getenv("HIVE_CONF_DIR") != null) {
        String hivercDefault = System.getenv("HIVE_CONF_DIR") + File.separator
          + HIVERCFILE;
        if (new File(hivercDefault).exists()) {
          int rc = processFile(hivercDefault);
          if (rc != 0) {
            System.exit(rc);
          }
        }
      }
      if (System.getProperty("user.home") != null) {
        String hivercUser = System.getProperty("user.home") + File.separator +
          HIVERCFILE;
        if (new File(hivercUser).exists()) {
          int rc = processFile(hivercUser);
          if (rc != 0) {
            System.exit(rc);
          }
        }
      }
    }
    ss.setIsSilent(saveSilent);
  }

  public void processSelectDatabase(CliSessionState ss) throws IOException {
    String database = ss.database;
    if (database != null) {
      int rc = processLine("use " + database + ";");
      if (rc != 0) {
        System.exit(rc);
      }
    }
  }

  public static Completer[] getCommandCompleter() {
    // StringsCompleter与预定义的单词列表匹配. 我们从一个空白的单词开始，并构建它
    // StringsCompleter matches against a pre-defined wordlist
    // We start with an empty wordlist and build it up
    List<String> candidateStrings = new ArrayList<String>();

    // 添加函数, 如果函数只包含字母和下划线则在函数名最后添加一个'('
    // We add Hive function names
    // For functions that aren't infix operators, we add an open
    // parenthesis at the end.
    for (String s : FunctionRegistry.getFunctionNames()) {
      if (s.matches("[a-z_]+")) {
        candidateStrings.add(s + "(");
      } else {
        candidateStrings.add(s);
      }
    }

    // 添加关键词(包括大小写), 如: PARTITION, UPDATE, ALTER
    // We add Hive keywords, including lower-cased versions
    for (String s : HiveParser.getKeywords()) {
      candidateStrings.add(s);
      candidateStrings.add(s.toLowerCase());
    }

    StringsCompleter strCompleter = new StringsCompleter(candidateStrings);

    // 因为除了空格以外我们还使用括号作为关键字分隔符，我们需要定义一个新的ArgumentDelimiter，它将'(', ')', '[', ']', ' '识别为分隔符。
    // Because we use parentheses in addition to whitespace
    // as a keyword delimiter, we need to define a new ArgumentDelimiter
    // that recognizes parenthesis as a delimiter.
    ArgumentDelimiter delim = new AbstractArgumentDelimiter() {
      @Override
      public boolean isDelimiterChar(CharSequence buffer, int pos) {
        char c = buffer.charAt(pos);
        return (Character.isWhitespace(c) || c == '(' || c == ')' ||
            c == '[' || c == ']');
      }
    };

    // The ArgumentCompletor allows us to match multiple tokens
    // in the same line.
    final ArgumentCompleter argCompleter = new ArgumentCompleter(delim, strCompleter);
    // By default ArgumentCompletor is in "strict" mode meaning
    // a token is only auto-completed if all prior tokens
    // match. We don't want that since there are valid tokens
    // that are not in our wordlist (eg. table and column names)
    argCompleter.setStrict(false);

    // ArgumentCompletor总是在匹配的令牌之后添加一个空格。 这对于函数名称是不希望的，因为开头括号后的空格在Hive中是不必要的（并且不常见）。 我们将自定义Completor放在我们的ArgumentCompletor之上，以扭转这一点。
    // ArgumentCompletor always adds a space after a matched token.
    // This is undesirable for function names because a space after
    // the opening parenthesis is unnecessary (and uncommon) in Hive.
    // We stack a custom Completor on top of our ArgumentCompletor
    // to reverse this.
    Completer customCompletor = new Completer () {
      @Override
      public int complete (String buffer, int offset, List completions) {
        List<String> comp = completions;
        int ret = argCompleter.complete(buffer, offset, completions);
        // ConsoleReader will do the substitution if and only if there
        // is exactly one valid completion, so we ignore other cases.
        if (completions.size() == 1) {
          if (comp.get(0).endsWith("( ")) {
            comp.set(0, comp.get(0).trim());
          }
        }
        return ret;
      }
    };

    List<String> vars = new ArrayList<String>();
    // 获取HiveConf所有的配置项, 如: hive.jar.path
    for (HiveConf.ConfVars conf : HiveConf.ConfVars.values()) {
      vars.add(conf.varname);
    }

    StringsCompleter confCompleter = new StringsCompleter(vars) {
      @Override
      public int complete(final String buffer, final int cursor, final List<CharSequence> clist) {
        int result = super.complete(buffer, cursor, clist);
        if (clist.isEmpty() && cursor > 1 && buffer.charAt(cursor - 1) == '=') {
          HiveConf.ConfVars var = HiveConf.getConfVars(buffer.substring(0, cursor - 1));
          if (var == null) {
            return result;
          }
          if (var.getValidator() instanceof Validator.StringSet) {
            Validator.StringSet validator = (Validator.StringSet)var.getValidator();
            clist.addAll(validator.getExpected());
          } else if (var.getValidator() != null) {
            clist.addAll(Arrays.asList(var.getValidator().toDescription(), ""));
          } else {
            clist.addAll(Arrays.asList("Expects " + var.typeString() + " type value", ""));
          }
          return cursor;
        }
        if (clist.size() > DELIMITED_CANDIDATE_THRESHOLD) {
          Set<CharSequence> delimited = new LinkedHashSet<CharSequence>();
          for (CharSequence candidate : clist) {
            Iterator<String> it = Splitter.on(".").split(
                candidate.subSequence(cursor, candidate.length())).iterator();
            if (it.hasNext()) {
              String next = it.next();
              if (next.isEmpty()) {
                next = ".";
              }
              candidate = buffer != null ? buffer.substring(0, cursor) + next : next;
            }
            delimited.add(candidate);
          }
          clist.clear();
          clist.addAll(delimited);
        }
        return result;
      }
    };

    StringsCompleter setCompleter = new StringsCompleter("set") {
      @Override
      public int complete(String buffer, int cursor, List<CharSequence> clist) {
        return buffer != null && buffer.equals("set") ? super.complete(buffer, cursor, clist) : -1;
      }
    };

    ArgumentCompleter propCompleter = new ArgumentCompleter(setCompleter, confCompleter) {
      @Override
      public int complete(String buffer, int offset, List<CharSequence> completions) {
        int ret = super.complete(buffer, offset, completions);
        if (completions.size() == 1) {
          completions.set(0, ((String)completions.get(0)).trim());
        }
        return ret;
      }
    };
    return new Completer[] {propCompleter, customCompletor};
  }

  // hive cli执行的入口
  public static void main(String[] args) throws Exception {
    // 调用CliDriver的构造方法, 初始化conf和console属性
    int ret = new CliDriver().run(args);
    System.exit(ret);
  }

  public  int run(String[] args) throws Exception {
    // 利用apache commond cli来处理输入参数中的hiveconf, define和hivevar, 如果处理出错返回false, 否则返回true
    OptionsProcessor oproc = new OptionsProcessor();
    if (!oproc.process_stage1(args)) {
      return 1;
    }

    // 重置默认的log4j配置并为hive重新初始化log4j, 注意, 在这里是读取hive-log4j.properties来初始化log4j
    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    boolean logInitFailed = false;
    String logInitDetailMessage;
    try {
      logInitDetailMessage = LogUtils.initHiveLog4j();
    } catch (LogInitializationException e) {
      logInitFailed = true;
      logInitDetailMessage = e.getMessage();
    }

    // 创建CliSessionState, 并初始化in, out, error等stream流. CliSessionState是一次命令行操作的session会话, 其继承了SessionState
    CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
    ss.in = System.in;
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8");
      ss.info = new PrintStream(System.err, true, "UTF-8");
      ss.err = new CachingPrintStream(System.err, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return 3;
    }

    // 处理从命令行读取其它参数, 并将相应的信息设置到CliSessionState中, 处理成功返回true, 否则返回false
    if (!oproc.process_stage2(ss)) {
      return 2;
    }

    // 当前会话是否在 silent 模式运行。 如果不是silent模式(默认, 即hive.session.silent值为false)，所有 info 级打在日志中的消息，都将以标准错误流的形式输出到控制台。
    // 如果是silent模式(即hive.session.silent值为true), 日志信息不会打印到控制台
    if (!ss.getIsSilent()) {
      if (logInitFailed) {
        // 初始化失败, 通过System.err在控制台打印错误信息
        System.err.println(logInitDetailMessage);
      } else {
        // 初始化成功, 通过SessionState的_console调用ss.info在控制台打印日志初始化成功信息, 实际调用的是System.err, 因为上面已经ss.info = new PrintStream(System.err, true, "UTF-8");
        // 如: Logging initialized using configuration in file:/export/App/hive-1.2.1/conf/hive-log4j.properties
        SessionState.getConsole().printInfo(logInitDetailMessage);
      }
    }

    // 将用户输入的 -hiveconf key=value , 各个key value设置到conf中, 这会覆盖conf中相同参数名的值. 然后吧这些值赋值给SessionState的overriddenConfigurations
    // set all properties specified via command line
    HiveConf conf = ss.getConf();
    for (Map.Entry<Object, Object> item : ss.cmdProperties.entrySet()) {
      conf.set((String) item.getKey(), (String) item.getValue());
      ss.getOverriddenConfigurations().put((String) item.getKey(), (String) item.getValue());
    }

    // read prompt configuration and substitute variables.
    // cli输出的提示符, 默认值为hive
    prompt = conf.getVar(HiveConf.ConfVars.CLIPROMPT);
    // 对prompt进行变量置换, 比如prompt是"hive${A}", SessionState的hiveVariables有有<"A", "-->">, 则prompt变为hive-->
    prompt = new VariableSubstitution(new HiveVariableSource() {
      @Override
      public Map<String, String> getHiveVariable() {
        return SessionState.get().getHiveVariables();
      }
    }).substitute(conf, prompt);
    // 获取与prompt长度相同的空字符串
    prompt2 = spacesForString(prompt);

    // 默认值为true, 启动SessionState
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_CLI_TEZ_SESSION_ASYNC)) {
      // Start the session in a fire-and-forget manner. When the asynchronously initialized parts of
      // the session are needed, the corresponding getters and other methods will wait as needed.
      SessionState.beginStart(ss, console);
    } else {
      SessionState.start(ss);
    }

    ss.updateThreadName();

    // 执行逻辑在executeDriver
    // execute cli driver work
    try {
      return executeDriver(ss, conf, oproc);
    } finally {
      ss.resetThreadName();
      ss.close();
    }
  }

  /**
   * Execute the cli work
   * @param ss CliSessionState of the CLI driver
   * @param conf HiveConf for the driver session
   * @param oproc Operation processor of the CLI invocation
   * @return status of the CLI command execution
   * @throws Exception
   */
  private int executeDriver(CliSessionState ss, HiveConf conf, OptionsProcessor oproc)
      throws Exception {

    CliDriver cli = new CliDriver();
    // 将用户在命令行中通过define和hivevar定义的变量赋值给当前线程的SessionState的hiveVariables属性
    cli.setHiveVariables(oproc.getHiveVariables());

    // 如果在启动时 在命令行指定了 -database dbname, 则设置为CliDriver的database
    // use the specified database if specified
    cli.processSelectDatabase(ss);

    // Execute -i init files (always in silent mode), 先执行-i filename中指定的hql, 看作是一种初始化操作(初始化一些参数或表信息)
    cli.processInitFiles(ss);

    // 如果指定了 -e, 则直接执行该hql
    if (ss.execString != null) {
      int cmdProcessStatus = cli.processLine(ss.execString);
      return cmdProcessStatus;
    }

    // 如果指定了 -f, 则直接执行文件中的hql
    try {
      if (ss.fileName != null) {
        return cli.processFile(ss.fileName);
      }
    } catch (FileNotFoundException e) {
      System.err.println("Could not open input file for reading. (" + e.getMessage() + ")");
      return 3;
    }
    // 如果使用的是默认的mr引擎, 则打印提示信息
    if ("mr".equals(HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE))) {
      console.printInfo(HiveConf.generateMrDeprecationWarning());
    }

    // 如果没有-e或-f, 则说明是交互式的方式启动的。那么就会调用setupConsoleReader(), setupConsoleReader方法会初始化ConsoleReader reader,
    // reader实际是一个命令行工具, 设置reader的各种提示符及历史记录保存文件
    setupConsoleReader();

    String line;
    int ret = 0;
    String prefix = "";
    // 默认返回""
    String curDB = getFormattedDb(conf, ss);
    // prompt默认值是hive, 所以curPrompt默认值为: hive
    String curPrompt = prompt + curDB;
    // 返回与curDB长度相同的空格
    String dbSpaces = spacesForString(curDB);

    // 终端输出提示符: curPrompt + "> ", 通常是: hive>, while循环的逻辑就是循环读取用户输入的hql执行
    while ((line = reader.readLine(curPrompt + "> ")) != null) {
      if (!prefix.equals("")) {
        prefix += '\n';
      }
      // 如果当前行以"--"开头, 则忽略该行
      if (line.trim().startsWith("--")) {
        continue;
      }
      // 如果当前行以':'结尾, 且不是已'\\;'结尾, 则认为用户已经输入完一条完整的sql, 调用processLine方法执行该sql.
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line;
        // 最最核心的地方, processLine方法, true表示可以通过ctrl + c进行中断, 这里的line是带着';'的
        ret = cli.processLine(line, true);
        // 处理完成后, prefix再置为"", 即初始化再次等待用户输入
        prefix = "";
        curDB = getFormattedDb(conf, ss);
        curPrompt = prompt + curDB;
        dbSpaces = dbSpaces.length() == curDB.length() ? dbSpaces : spacesForString(curDB);
      } else {
        prefix = prefix + line;
        // prompt2默认值是与prompt长度相同的空格串
        curPrompt = prompt2 + dbSpaces;
        continue;
      }
    }

    return ret;
  }

  private void setupCmdHistory() {
    final String HISTORYFILE = ".hivehistory";
    String historyDirectory = System.getProperty("user.home");
    PersistentHistory history = null;
    try {
      if ((new File(historyDirectory)).exists()) {
        // 创建文件 /home/username/.hivehistory 用来保存历史查询记录
        String historyFile = historyDirectory + File.separator + HISTORYFILE;
        history = new FileHistory(new File(historyFile));
        reader.setHistory(history);
      } else {
        System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.");
      }
    } catch (Exception e) {
      System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                         "history file.  History will not be available during this session.");
      System.err.println(e.getMessage());
    }

    // 增加一个刷新的钩子程序, 关闭的时候将内存中的查询历史刷新到.hivehistory文件中
    // add shutdown hook to flush the history to history file
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        History h = reader.getHistory();
        if (h instanceof FileHistory) {
          try {
            ((FileHistory) h).flush();
          } catch (IOException e) {
            System.err.println("WARNING: Failed to write command history file: " + e.getMessage());
          }
        }
      }
    }));
  }

  protected void setupConsoleReader() throws IOException {
    reader = new ConsoleReader();
    reader.setExpandEvents(false);
    reader.setBellEnabled(false);
    //  JLine中跟自动补全相关的接口是Completer, 每个completer代表一类自动补全规则, 添加completer的顺序就是调用自动补全规则的顺序
    for (Completer completer : getCommandCompleter()) {
      // 将各种自动补全的completer添加到reader
      reader.addCompleter(completer);
    }
    // 可以通过ConsoleReader的setUseHistory(boolean useHistory)方法启用/禁用Command History功能。
    // ConsoleReader的history成员变量负责保存历史数据，默认情况下历史数据只保存在内存中。如果希望将历史数据保存 到文件中，那么只需要以File对象作为参数构造History对象，并将该History对象设置到ConsoleReader即可。
    setupCmdHistory();
  }

  /**
   * Retrieve the current database name string to display, based on the
   * configuration value.
   * @param conf storing whether or not to show current db
   * @param ss CliSessionState to query for db name
   * @return String to show user for current db value
   */
  private static String getFormattedDb(HiveConf conf, CliSessionState ss) {
    // 提示符中是否包含当前的数据库名, 默认是false
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIPRINTCURRENTDB)) {
      return "";
    }
    //BUG: This will not work in remote mode - HIVE-5153
    String currDb = SessionState.get().getCurrentDatabase();

    if (currDb == null) {
      return "";
    }

    return " (" + currDb + ")";
  }

  /**
   * Generate a string of whitespace the same length as the parameter
   *
   * @param s String for which to generate equivalent whitespace
   * @return  Whitespace
   */
  private static String spacesForString(String s) {
    if (s == null || s.length() == 0) {
      return "";
    }
    return String.format("%1$-" + s.length() +"s", "");
  }

  public void setHiveVariables(Map<String, String> hiveVariables) {
    SessionState.get().setHiveVariables(hiveVariables);
  }

}
