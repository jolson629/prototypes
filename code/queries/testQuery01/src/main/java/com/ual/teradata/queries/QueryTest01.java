package com.ual.teradata.queries;

import com.beust.jcommander.JCommander;
import org.apache.log4j.Logger;
import java.sql.*;

// Source: https://developer.teradata.com/doc/connectivity/jdbc/reference/current/samplePrograms.html
// Teradata source jars: https://community.teradata.com/t5/General/pom-file-for-maven-project/td-p/27252


// Simple jdbc connectivity test with Teradata using the (proprietary) Teradata jdbc driver.

public class QueryTest01 {

    final static Logger logger = Logger.getLogger(QueryTest01.class);

    public static void main(String [] args)
    throws ClassNotFoundException
    {

        TeradataArgs config = new TeradataArgs();
        JCommander.newBuilder()
                .addObject(config)
                .build()
               .parse(args);

        logger.info("Starting run....");

        // Teradata Type 4 JDBC Driver. Use LDAP for authentication
        String url = "jdbc:teradata://"+config.teradataHost+"/LOGMECH=LDAP,TMODE=ANSI,CHARSET=UTF8";
        String sSql = "Select count(*) from "+config.database+"."+config.table;

        try {

            // Load the Teradata Driver
            Class.forName ("com.teradata.jdbc.TeraDriver");

            // Connect to the Teradata database specified in the URL
            // and submit userid and password.
            logger.info("Connecting to " + url);
            Connection con = DriverManager.getConnection (url, config.user, config.password);
            logger.info("Established successful connection");

            Statement stmt = con.createStatement();
            logger.info("Executing sql statement: "+sSql);
            ResultSet rset = stmt.executeQuery(sSql);
            while (rset.next()) {
                logger.info("Row Count: "+rset.getInt(1));
            }
            con.close();
            logger.info("Disconnected");
        }
        catch (SQLException ex)
        {
            // A SQLException was generated.  Catch it and display
            // the error information.
            // Note that there could be multiple error objects chained
            // together.

            logger.error("*** SQLException caught ***");

            while (ex != null)
            {
                logger.error(" Error code: " + ex.getErrorCode());
                logger.error(" SQL State: " + ex.getSQLState());
                logger.error(" Message: " + ex.getMessage());
                logger.error("");
                ex = ex.getNextException();
            }

            throw new IllegalStateException ("Sample failed.") ;
        }
    }

}
