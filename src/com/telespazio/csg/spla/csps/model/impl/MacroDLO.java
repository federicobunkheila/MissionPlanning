/**
*
* MODULE FILE NAME: MacroDLO.java
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

package com.telespazio.csg.spla.csps.model.impl;

import java.util.ArrayList;
import java.util.Date;

import com.telespazio.csg.spla.csps.model.IMacroDLO;

import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;

/**
 * The MacroDLO class which implements the relevant interface.
 */
public class MacroDLO implements IMacroDLO {

	/**
	 * The macro Id
	 */
	private String macroId;

	/**
	 * The sequence Id
	 */
	private int packetSeqId;

	/**
	 * The satellite Id
	 */
	private String satId;

	/**
	 * The macro start time
	 */
	private Date startTime;

	/**
	 * The macro stop time
	 */
	private Date stopTime;

	/**
	 * The visibility list
	 */
	private ArrayList<Visibility> visList;

	/**
	 * The default constructor
	 */
	public MacroDLO() {

	}

	/**
	 * The filled constructor
	 *
	 * @param macroId
	 * @param satId
	 * @param startTime
	 * @param stopTime
	 * @param visList
	 */
	public MacroDLO(String macroId, String satId, Date startTime, Date stopTime, ArrayList<Visibility> visList) {

		super();
		this.macroId = macroId;
		this.satId = satId;
		this.startTime = startTime;
		this.stopTime = stopTime;
		this.visList = visList;
	}

	/**
	 * @return the macroId
	 */
	@Override
	public String getMacroId() {
		return this.macroId;
	}

	/**
	 * @param macroId
	 *            the macroId to set
	 */
	@Override
	public void setMacroId(String macroId) {
		this.macroId = macroId;
	}

	/**
	 * @return the packetSeqId
	 */
	@Override
	public int getPacketSeqId() {
		return this.packetSeqId;
	}

	/**
	 * @param packetSeqId
	 *            the packetSeqId to set
	 */
	@Override
	public void setPacketSeqId(Integer packetSeqId) {
		this.packetSeqId = packetSeqId;
	}

	/**
	 * @return the satId
	 */
	@Override
	public String getSatId() {
		return this.satId;
	}

	/**
	 * @param satId
	 *            the satId to set
	 */
	@Override
	public void setSatId(String satId) {
		this.satId = satId;
	}

	/**
	 * @return the startTime
	 */
	@Override
	public Date getStartTime() {
		return this.startTime;
	}

	/**
	 * @param startTime
	 *            the startTime to set
	 */
	@Override
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	/**
	 * @return the stopTime
	 */
	@Override
	public Date getStopTime() {
		return this.stopTime;
	}

	/**
	 * @param stopTime
	 *            the stopTime to set
	 */
	@Override
	public void setStopTime(Date stopTime) {
		this.stopTime = stopTime;
	}

	/**
	 * @return the visList
	 */
	@Override
	public ArrayList<Visibility> getVisList() {
		return this.visList;
	}

	/**
	 * @param visList
	 *            the visList to set
	 */
	@Override
	public void setVisList(ArrayList<Visibility> visList) {
		this.visList = visList;
	}
}
