/**
*
* MODULE FILE NAME: ConfigProperties.java
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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * The configuration class of the CSPS properties.
 */
public class CSPSProperties {

	private final Properties prop;

	/**
	 *
	 */
	public CSPSProperties() {
		this.prop = new Properties();
	}

	/**
	 *
	 * @param configFilePath
	 * @throws Exception
	 */
	public void init(String configFilePath) throws Exception {
		
		// Load properties
		loadPropertiesFile(configFilePath);
	}

	/**
	 * Load File properties throw ConfigurationException
	 *
	 * @param propertiesFilePath
	 * @throws Exception
	 */
	private void loadPropertiesFile(String propertiesFilePath) throws Exception {
		
		/**
		 * The input stream
		 */
		InputStream input = null;
		try {
			/**
			 * The file input
			 */
			input = new FileInputStream(propertiesFilePath);
			this.prop.load(input);
			
			// Close input
			input.close();
		} catch (Exception e) {
			throw new Exception(" read " + propertiesFilePath + " Failed");
		}
	}

	/**
	 * Get given property name
	 *
	 * @param propName
	 * @return
	 * @throws Exception
	 */
	public String getProperty(String propName) throws Exception {
		
		/**
		 * The property value
		 */		
		String value = this.prop.getProperty(propName);
		
		// Check null value
		if (value == null || value.isEmpty()) {
			
			throw new Exception(propName + " not configured.");
		}

		return value;
	}

	/**
	 * Contain given property name
	 *
	 * @param propName
	 * @return
	 * @throws Exception
	 */
	public boolean containsProperties(String propName) throws Exception {
		
		/**
		 * The container boolean
		 */
		boolean isContained = this.prop.containsKey(propName);

		return isContained;
	}


}
