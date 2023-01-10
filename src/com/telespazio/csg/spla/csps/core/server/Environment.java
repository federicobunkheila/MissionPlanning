/**
*
* MODULE FILE NAME: com.telespazio.csg.spla.csps.core.server.Environment.java
*
* MODULE TYPE:      <Class definition>
*
* FUNCTION:         <Functional description of the DDC>
*
* PURPOSE:          <List of SR>
*
* CREATION DATE:    <01-Jan-2017>
*
* AUTHORS:          bunkheila Bunkheila
*
* DESIGN ISSUE:     1.0
*
* INTERFACES:       <prototype and list of input/output parameters>
*
* SUBORDINATES:     <list of functions called by this DDC>
*
* MODIFICATION HISTORY:
*
*             Date          |  Name      |   New ver.     | Description
* --------------------------+------------+----------------+-------------------------------
* <DD-MMM-YYYY>             | <name>     |<Ver>.<Rel>     | <reasons of changes>
* --------------------------+------------+----------------+-------------------------------
*
* PROCESSING
*/

package com.telespazio.csg.spla.csps.core.server;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

import it.sistematica.spla.datamodel.core.exception.ConfigurationException;

/**
 * The CSPS com.telespazio.csg.spla.csps.core.server.Environment class.
 *
 * @author bunkheila
 */
public class Environment {

	public static final String SPLA_ENV = System.getenv("SPLA_ENV");
	public static final String SPLA_HOME = System.getenv("SPLA_HOME");

	public static final String CONFIG_DIR = SPLA_HOME + File.separator + "CSPS" + File.separator + "CONFIG"
			+ File.separator;
	public static final String DATA_DIR = SPLA_HOME + File.separator + "CSPS" + File.separator + "DATA"
			+ File.separator;
	public static final String CSPS_PROPS_DIR = CONFIG_DIR + "csps.properties";

	private static Configuration configuration;

	public static String getEnv() throws ConfigurationException {
		String ret = System.getenv(SPLA_ENV);
		// if (Util.isEmpty(ret))
		// {
		// throw new ConfigurationException("com.telespazio.csg.spla.csps.core.server.Environment variable " +
		// com.telespazio.csg.spla.csps.core.server.Environment + " not set");
		// }
		return Util.isNullReplace(ret).trim();
	}

	public static String getHomeDir() throws ConfigurationException {
		String ret = System.getenv(SPLA_HOME);
		// if (Util.isEmpty(ret))
		// {
		// throw new ConfigurationException("com.telespazio.csg.spla.csps.core.server.Environment variable " +
		// SPLA_HOME_DIR + " not set");
		// }
		return Util.adjustPath(Util.isNullReplace(ret).trim());
	}

	/**
	 * The file path specified as parameter will be appended to the value of the
	 * com.telespazio.csg.spla.csps.core.server.Environment variable <code>SPLA_HOME</code>
	 *
	 * @param filePath
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws DOMException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws ConfigurationException
	 */
	private static void readConfiguration(String filePath)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, DOMException,
			ParserConfigurationException, SAXException, IOException, ConfigurationException {

		System.out.println("");
		System.out.println("Variable " + SPLA_ENV + "  : [" + getEnv() + "]");
		System.out.println("Variable " + SPLA_HOME + " : [" + getHomeDir() + "]");
		System.out.println("");

		/**
		 * The configuration
		 */
		configuration = new Configuration();
		
		// Init configuration
		configuration.init(getHomeDir() + filePath);
	}

	/**
	 * Initialize configuration
	 */
	public static void initConfiguration()
			throws ConfigurationException, InstantiationException, IllegalAccessException, ClassNotFoundException,
			DOMException, ParserConfigurationException, SAXException, IOException {

		// Configure log4j properties	
		PropertyConfigurator.configure(CONFIG_DIR + "log4j.properties");
		
		// Read configuration
		readConfiguration(CONFIG_DIR + "csps.xml");
	}

	/**
	 * Get configuration
	 * 
	 * @return configuration
	 */
	public static Configuration getConfiguration() {
		return configuration;
	}
}
