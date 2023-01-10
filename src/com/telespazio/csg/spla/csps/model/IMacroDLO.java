/**
*
* MODULE FILE NAME: IMacroDLO.java
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

package com.telespazio.csg.spla.csps.model;

import java.util.ArrayList;
import java.util.Date;

import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;

/**
 * The MacroDLO Interface.
 */
public interface IMacroDLO {

	/**
	 * @return the macroId
	 */
	String getMacroId();

	/**
	 * @param macroId
	 *            the macroId to set
	 */
	void setMacroId(String macroId);

	/**
	 * @return the packetSeqId
	 */
	int getPacketSeqId();

	/**
	 * @param seqId
	 *            the packetSeqId to set
	 */
	void setPacketSeqId(Integer seqId);

	/**
	 * @return the satId
	 */
	String getSatId();

	/**
	 * @param satId
	 *            the satId to set
	 */
	void setSatId(String satId);

	/**
	 * @return the macro startTime
	 */
	Date getStartTime();

	/**
	 * @param startTime
	 *            the macro startTime to set
	 */
	void setStartTime(Date stopTime);

	/**
	 * @return the macro stopTime
	 */
	Date getStopTime();

	/**
	 * @param stopTime
	 *            the macro stopTime to set
	 */
	void setStopTime(Date startTime);

	/**
	 * @return the visList
	 */
	ArrayList<Visibility> getVisList();

	/**
	 * @param visList
	 *            the visList to set
	 */
	void setVisList(ArrayList<Visibility> visList);

}
