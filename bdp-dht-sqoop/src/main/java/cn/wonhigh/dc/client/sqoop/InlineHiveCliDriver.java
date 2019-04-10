package cn.wonhigh.dc.client.sqoop;

import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 * <p>
 * This class is copied from org.apache.hadoop.hive.cli.CliDriver with
 * removing log4j related initialization to avoid deadlocks due to conflict
 * between application such as bdp-dht with hive's log4j usage.
 */
public class InlineHiveCliDriver extends CliDriver {
    private static final Logger logger = Logger.getLogger(InlineHiveCliDriver.class);

    public InlineHiveCliDriver() {
        super();
    }

    @Override
    public int run(String[] args) throws Exception {

        OptionsProcessor oproc = new OptionsProcessor();
        if (!oproc.process_stage1(args)) {
            return 1;
        }

        // NOTE: It is critical to do this here so that log4j is reinitialized
        // before any of the other core hive classes are loaded

        // Qiuzhuang: we don't use this to avoid deadlock for log4j
    /*
    boolean logInitFailed = false;
    String logInitDetailMessage;
    try {
      logInitDetailMessage = LogUtils.initHiveLog4j();
    } catch (LogUtils.LogInitializationException e) {
      logInitFailed = true;
      logInitDetailMessage = e.getMessage();
    }
    */
        CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
        ss.in = System.in;
        try {
            ss.out = new PrintStream(System.out, true, "UTF-8");
            ss.info = new PrintStream(System.err, true, "UTF-8");
            ss.err = new CachingPrintStream(System.err, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return 3;
        }

        if (!oproc.process_stage2(ss)) {
            return 2;
        }

    /*
    if (!ss.getIsSilent()) {
      if (logInitFailed) {
        System.err.println(logInitDetailMessage);
      } else {
        SessionState.getConsole().printInfo(logInitDetailMessage);
      }
    }
    */

        // set all properties specified via command line
        HiveConf conf = ss.getConf();
        for (Map.Entry<Object, Object> item : ss.cmdProperties.entrySet()) {
            conf.set((String) item.getKey(), (String) item.getValue());
            ss.getOverriddenConfigurations().put((String) item.getKey(), (String) item.getValue());
        }

        // read prompt configuration and substitute variables.
        prompt = conf.getVar(HiveConf.ConfVars.CLIPROMPT);
        //replace hive 2.1.1 modify
//        prompt = new VariableSubstitution(new HiveVariableSource(){
//        public Map<String, String> getHiveVariable() {
//            logger.info("--------------------------------------");
//            return null;
//        }
//        }).substitute(conf, prompt);
        //hive 1.2.1
        prompt = new VariableSubstitution().substitute(conf, prompt);
        prompt2 = spacesForString(prompt);

        SessionState.start(ss);

        // execute cli driver work
        try {
            return executeDriver(ss, conf, oproc);
        } finally {
            ss.close();
        }
    }

    /**
     * Execute the cli work
     *
     * @param ss    CliSessionState of the CLI driver
     * @param conf  HiveConf for the driver session
     * @param oproc Operation processor of the CLI invocation
     * @return status of the CLI command execution
     * @throws Exception
     */
    private int executeDriver(CliSessionState ss, HiveConf conf, OptionsProcessor oproc)
            throws Exception {

        InlineHiveCliDriver cli = new InlineHiveCliDriver();
        cli.setHiveVariables(oproc.getHiveVariables());

        // use the specified database if specified
        cli.processSelectDatabase(ss);

        // Execute -i init files (always in silent mode)
        cli.processInitFiles(ss);

        if (ss.execString != null) {
            int cmdProcessStatus = cli.processLine(ss.execString);
            return cmdProcessStatus;
        }

        try {
            if (ss.fileName != null) {
                return cli.processFile(ss.fileName);
            }
        } catch (FileNotFoundException e) {
            System.err.println("Could not open input file for reading. (" + e.getMessage() + ")");
            return 3;
        }

        setupConsoleReader();

        String line;
        int ret = 0;
        String prefix = "";
        String curDB = getFormattedDb(conf, ss);
        String curPrompt = prompt + curDB;
        String dbSpaces = spacesForString(curDB);

        while ((line = reader.readLine(curPrompt + "> ")) != null) {
            if (!prefix.equals("")) {
                prefix += '\n';
            }
            if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
                line = prefix + line;
                ret = cli.processLine(line, true);
                prefix = "";
                curDB = getFormattedDb(conf, ss);
                curPrompt = prompt + curDB;
                dbSpaces = dbSpaces.length() == curDB.length() ? dbSpaces : spacesForString(curDB);
            } else {
                prefix = prefix + line;
                curPrompt = prompt2 + dbSpaces;
                continue;
            }
        }

        return ret;
    }

    /**
     * Retrieve the current database name string to display, based on the
     * configuration value.
     *
     * @param conf storing whether or not to show current db
     * @param ss   CliSessionState to query for db name
     * @return String to show user for current db value
     */
    private static String getFormattedDb(HiveConf conf, CliSessionState ss) {
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
     * @return Whitespace
     */
    private static String spacesForString(String s) {
        if (s == null || s.length() == 0) {
            return "";
        }
        return String.format("%1$-" + s.length() + "s", "");
    }
}
