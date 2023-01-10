/**
*
* MODULE FILE NAME: Util.java
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

/**
 *
 * The CSPS com.telespazio.csg.spla.csps.core.server.Environment class.
 *
 */
public class Util {

	/**
	 * Sleep.
	 *
	 * @param milliseconds
	 *            the milliseconds
	 */
	public static void sleep(int milliseconds) {
		try {
			// Sleep for
			// the specified milliseconds
			Thread.sleep(milliseconds);
		} catch (InterruptedException ie) {

			// Restore interrupt flag
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Checks if is empty.
	 *
	 * @param s
	 *            the s
	 * @return true, if is empty
	 */
	public static boolean isEmpty(String s) {
		// check if a string is null or empty
		return ((s == null) || (s.trim().length() == 0));
	}

	/**
	 * Checks if is null replace.
	 *
	 * @param s
	 *            the s
	 * @return the string
	 */
	public static String isNullReplace(String s) {
		// check if a string is null or empty and
		// replaces it with empty string
		return isNullReplace(s, "");
	}

	/**
	 * Checks if is null replace.
	 *
	 * @param s
	 *            the s
	 * @param replacement
	 *            the replacement
	 * @return the string
	 */
	public static String isNullReplace(String s, String replacement) {
		// check if a string is null or empty and
		// replaces it with string <code>replacement<code>
		return ((s == null) || (s.trim().length() == 0)) ? replacement : s;
	}

	/**
	 * Adjust path. Adds a <code>File.separator</code> char to the end of the string
	 * if not present
	 *
	 * @param path
	 *            the path
	 * @return the string
	 */
	public static String adjustPath(String path) {
		// adds a File.separator char to the end of the string if not present
		return isNullReplace(path) + (isNullReplace(path).endsWith(File.separator) ? "" : File.separator);
	}
}
