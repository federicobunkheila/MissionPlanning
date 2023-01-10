/**
*
* MODULE FILE NAME: SchedDTO.java
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.telespazio.csg.spla.csps.model.ISchedDTO;

import it.sistematica.spla.datamodel.core.enums.DTOLinkType;
import it.sistematica.spla.datamodel.core.enums.DTOSensorMode;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.PRType;
import it.sistematica.spla.datamodel.core.model.UserInfo;
import it.sistematica.spla.datamodel.core.model.task.BITE;

/**
 * The SchedDTO class which implements the relevant interface.
 */
@SuppressWarnings("serial")
public class SchedDTO implements ISchedDTO, Serializable {

	/**
	 * The DTO Id
	 */
	private String dtoId;
	/**
	 * The EquivDTOId
	 */
	private String equivDTOId;
	/**
	 * The AR Id
	 */
	private String aRId;
	/**
	 * The PR Id
	 */
	private String pRId;
	/**
	 * The DTO start time
	 */
	private Date startTime;
	/**
	 * The DTO stop time
	 */
	private Date stopTime;
	/**
	 * The DTO sensor mode
	 */
	private DTOSensorMode sensorMode;
	/**
	 * The DTO sizeH
	 */
	private Double sizeH;
	/**
	 * The DTO sizeV
	 */
	private Double sizeV;
	/**
	 * The antenna beam Id
	 */
	private String beamId;

	/**
	 * The target distance
	 */
	private double targetDistance;

	/**
	 * The satellite to DTO angle
	 */
	private double lookAngle;

	/**
	 * The satellite to DTO look side
	 */
	private String lookSide;

	/**
	 * The satellite Id
	 */
	private String satId;

	/**
	 * The DTO BIC
	 */
	private double bIC;

	/**
	 * The DTO polarization
	 */
	private String polar;

	/**
	 * The DTO orbit direction
	 */
	private String orbDir;

	/**
	 * The DTO status
	 */
	private DtoStatus status;

	/**
	 * The linked DTO Id list
	 */
	private ArrayList<String> linkDTOIdList;

	/**
	 * The join DTO
	 */
	private String joinDTOId;

	/**
	 * The DTO link type
	 */
	private DTOLinkType dTOLinkType;

	/**
	 * The User Info list
	 */
	private List<UserInfo> userInfoList;
	/**
	 * The PRType of the DTO
	 */
	private PRType pRType;
	/**
	 * The DTO revolution number
	 */
	private int revNum;
	/**
	 * The NEO flag
	 */
	private boolean isNEO;
	/**
	 * The DI2S flag
	 */
	private boolean isDi2s;
	/**
	 * The PT flag
	 */
	private boolean isPTAvailable;
	/**
	 * The delta time
	 */
	private double deltaTime;

	/**
	 * The equivalent DTO flag
	 */
	private boolean isEquivalent;

	/**
	 * The DTO rank
	 */
	private int rank;

	/**
	 * The DTO priority
	 */
	private int priority;
	
	/**
	 * The DTO unique Id
	 */
	private String uniqueId;

	/**
	 * The preferred acquisition station Id list
	 */
	private List<String> prefStationIdList;

	/**
	 * The back acquisition station Id list
	 */
	private List<String> backStationIdList;
	
	/**
	 * The DTO pRMode
	 */
	private PRMode pRMode;

	/**
	 * The interleaved Di2s channel
	 */
	private int interleaved;

	/**
	 * The previously planned boolean
	 */
	private boolean isPrevPlanned;

	/**
	 * The previously paid boolean
	 */
	private boolean isDecrBIC;
	
	/**
	 * The time performance boolean
	 */
	private boolean isTimePerf;
	
	/**
	 * The bite
	 */
	private BITE bite;

	/**
	 * The replacing DTO Id list
	 */
	private ArrayList<String> replDTOIdList;
	
	/**
	 * The default constructor
	 */
	public SchedDTO() {

	}

	/**
	 * The filled constructor
	 *
	 * @param dtoId
	 * @param startTime
	 * @param stopTime
	 * @param sensorMode
	 * @param size
	 * @param beamId
	 * @param lookAngle
	 * @param lookSide
	 * @param satelliteId
	 * @param bIC
	 * @param orbDir
	 * @param status
	 * @param polar
	 * @param userInfoList
	 * @param prevARId
	 * @param pRType
	 * @param pRMode
	 * @param isNEO
	 * @param isPTAvailable
	 * @param deltaTime
	 */
	public SchedDTO(String dtoId, String equivDTOId, Date startTime, Date stopTime, DTOSensorMode sensorMode, Double sizeH, Double sizeV,
			String beamId, double lookAngle, String lookSide, String satelliteId, double bIC, String orbDir,
			DtoStatus status, String polar, List<UserInfo> userInfoList, String prevARId, PRType pRType, PRMode pRMode,
			boolean isNEO, boolean isPTAvailable) {
		super();
		this.dtoId = dtoId;
		this.equivDTOId = equivDTOId;
		this.startTime = startTime;
		this.stopTime = stopTime;
		this.sensorMode = sensorMode;
		this.sizeH = sizeH;
		this.sizeV = sizeV;
		this.beamId = beamId;
		this.lookAngle = lookAngle;
		this.lookSide = lookSide;
		this.satId = satelliteId;
		this.bIC = bIC;
		this.orbDir = orbDir;
		this.status = status;
		this.polar = polar;
		this.userInfoList = userInfoList;
		this.pRType = pRType;
		this.pRMode = pRMode;
		this.isNEO = isNEO;
		this.isPTAvailable = isPTAvailable;
	}

	/**
	 * @return the dtoId
	 */
	@Override
	public String getDTOId() {
		return this.dtoId;
	}

	/**
	 * @param dtoId
	 *            the dtoId to set
	 */
	@Override
	public void setDTOId(String dtoId) {
		this.dtoId = dtoId;
	}

	/**
	 * @return the equivDTOId
	 */
	public String getEquivDTOId() {
		return equivDTOId;
	}

	/**
	 * @param equivDTOId the equivDTOId to set
	 */
	public void setEquivDTOId(String equivDTOId) {
		this.equivDTOId = equivDTOId;
	}

	/**
	 * @return the aRId
	 */
	@Override
	public String getARId() {

		return this.aRId;
	}

	/**
	 * @param the
	 *            aRId to set
	 */
	@Override
	public void setARId(String aRId) {

		this.aRId = aRId;
	}

	/**
	 * @return the pRId
	 */
	@Override
	public String getPRId() {

		return this.pRId;
	}

	/**
	 * @param the
	 *            pRId to set
	 */
	@Override
	public void setPRId(String pRId) {

		this.pRId = pRId;
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
	 * @return the sensorMode
	 */
	@Override
	public DTOSensorMode getSensorMode() {
		return this.sensorMode;
	}

	/**
	 * @param sensorMode
	 *            the sensorMode to set
	 */
	@Override
	public void setSensorMode(DTOSensorMode sensorMode) {
		this.sensorMode = sensorMode;
	}

	/**
	 * @return the sizeH
	 */
	@Override
	public Double getSizeH() {
		return this.sizeH;
	}

	/**
	 * @param sizeH
	 *            the size to set
	 */
	@Override
	public void setSizeH(Double sizeH) {
		this.sizeH = sizeH;
	}

	/**
	 * @return the sizeV
	 */
	@Override
	public Double getSizeV() {
		return this.sizeV;
	}

	/**
	 * @param sizeV
	 *            the size to set
	 */
	@Override
	public void setSizeV(Double sizeV) {
		this.sizeV = sizeV;
	}

	/**
	 * @return the beamId
	 */
	@Override
	public String getBeamId() {
		return this.beamId;
	}

	/**
	 * @param beamId
	 *            the beamId to set
	 */
	@Override
	public void setBeamId(String beamId) {
		this.beamId = beamId;
	}

	/**
	 * @return the targetDistance
	 */
	public double getTargetDistance() {
		return this.targetDistance;
	}

	/**
	 * @param targetDistance
	 *            the targetDistance to set
	 */
	public void setTargetDistance(double targetDistance) {
		this.targetDistance = targetDistance;
	}

	/**
	 * @return the lookAngle
	 */
	public double getLookAngle() {
		return this.lookAngle;
	}

	/**
	 * @param lookAngle
	 *            the lookAngle to set
	 */
	public void setLookAngle(double lookAngle) {
		this.lookAngle = lookAngle;
	}

	/**
	 * @return the lookSide
	 */
	@Override
	public String getLookSide() {
		return this.lookSide;
	}

	/**
	 * @param lookSide
	 *            the lookSide to set
	 */
	@Override
	public void setLookSide(String lookSide) {
		this.lookSide = lookSide;
	}

	/**
	 * @return the satelliteId
	 */
	@Override
	public String getSatelliteId() {
		return this.satId;
	}

	/**
	 * @param satelliteId
	 *            the satelliteId to set
	 */
	@Override
	public void setSatelliteId(String satelliteId) {
		this.satId = satelliteId;
	}

	/**
	 * @return the DTO BIC
	 */
	@Override
	public double getBIC() {
		return this.bIC;
	}

	/**
	 * @param bIC
	 *            the DTO BIC to set
	 */
	@Override
	public void setBIC(double bIC) {
		this.bIC = bIC;
	}

	/**
	 * @return the orbDir
	 */
	public String getOrbDir() {
		return this.orbDir;
	}

	/**
	 * @param orbDir
	 *            the orbDir to set
	 */
	public void setOrbDir(String orbDir) {
		this.orbDir = orbDir;
	}

	/**
	 * @return the status
	 */
	@Override
	public DtoStatus getStatus() {
		return this.status;
	}

	/**
	 * @param status
	 *            the status to set
	 */
	@Override
	public void setStatus(DtoStatus status) {
		this.status = status;
	}

	/**
	 * @return the linkDTOIdList
	 */
	public ArrayList<String> getLinkDtoIdList() {
		return this.linkDTOIdList;
	}

	/**
	 * @param linkDTOIdList
	 *            the linkDTOId to set
	 */
	public void setLinkDtoIdList(ArrayList<String> linkDTOIdList) {
		this.linkDTOIdList = linkDTOIdList;
	}

	/**
	 * @return the joinDTOId
	 */
	public String getJoinDtoId() {
		return this.joinDTOId;
	}

	/**
	 * @param joinDto
	 *            the joinDTOId to set
	 */
	public void setJoinDtoId(String joinDTOId) {
		this.joinDTOId = joinDTOId;
	}

	/**
	 * @return the dtoLinkType
	 */
	public DTOLinkType getDTOLinkType() {
		return this.dTOLinkType;
	}

	/**
	 * @param dtoLinkType
	 *            the dtoLinkType to set
	 */
	public void setDTOLinkType(DTOLinkType dTOLinkType) {
		this.dTOLinkType = dTOLinkType;
	}

	/**
	 * @param polar
	 *            the polarization to set
	 */
	public void setPolarization(String polar) {
		this.polar = polar;
	}

	/**
	 * @return the polarization
	 */
	public String getPolarization() {
		return this.polar;
	}

	/**
	 * @return the userInfoList
	 */
	@Override
	public List<UserInfo> getUserInfoList() {
		return this.userInfoList;
	}

	/**
	 * @param userInfoList
	 *            the userInfoList to set
	 */
	@Override
	public void setUserInfoList(List<UserInfo> userInfoList) {
		this.userInfoList = userInfoList;
	}

	/**
	 * @return the pRType
	 */
	public PRType getPRType() {

		return this.pRType;
	}

	/**
	 * @param pRType
	 *            the pRType to set
	 */
	public void setPRType(PRType pRType) {

		this.pRType = pRType;
	}

	/**
	 * @return the interleaved
	 */
	public int getInterleaved() {
		return interleaved;
	}

	/**
	 * @param interleaved
	 *            the interleaved to set
	 */
	public void setInterleaved(int interleaved) {
		this.interleaved = interleaved;
	}

	/**
	 * @return the isNEO
	 */
	public boolean isNEO() {

		return this.isNEO;
	}

	/**
	 * @return the revNum
	 */
	public int getRevNum() {
		return this.revNum;
	}

	/**
	 * @param revNum
	 *            the revNum to set
	 */
	public void setRevNum(int revNum) {
		this.revNum = revNum;
	}

	/**
	 * @param isNEO
	 *            - the isNEO to set
	 */
	public void setNEOAvailable(boolean isNEO) {

		this.isNEO = isNEO;
	}

	/**
	 * @return the isDi2s
	 */
	public boolean isDi2sAvailable() {
		return this.isDi2s;
	}

	/**
	 * @param isDi2s
	 *            the isDi2s to set
	 */
	public void setDi2sAvailable(boolean isDi2s) {
		this.isDi2s = isDi2s;
	}

	/**
	 * @return the isPTAvailable to set
	 */
	public boolean isPTAvailable() {

		return this.isPTAvailable;
	}

	/**
	 * @param isPTAvailable
	 *            the isPTAvailable to set
	 */
	public void setPTAvailable(boolean isPTAvailable) {

		this.isPTAvailable = isPTAvailable;
	}

	/**
	 * @return the deltaTime
	 */
	public double getDeltaTime() {
		return this.deltaTime;
	}

	/**
	 * @param deltaTime
	 *            the deltaTime to set
	 */
	public void setDeltaTime(double deltaTime) {
		this.deltaTime = deltaTime;
	}

	/**
	 * @return the isEquivalent
	 */
	public boolean isEquivalent() {
		return this.isEquivalent;
	}

	/**
	 * @param isEquivalent
	 *            the isEquivalent to set
	 */
	public void setEquivalent(boolean isEquivalent) {
		this.isEquivalent = isEquivalent;
	}

	/**
	 * @return the rank
	 */
	public int getRank() {
		return this.rank;
	}

	/**
	 * @param rank
	 *            the rank to set
	 */
	public void setRank(int rank) {
		this.rank = rank;
	}
	
	/**
	 * @return the priority
	 */
	public int getPriority() {
		return priority;
	}

	/**
	 * @param priority the priority to set
	 */
	public void setPriority(int priority) {
		this.priority = priority;
	}

	/**
	 * @return the uniqueId
	 */
	public String getUniqueId() {
		return this.uniqueId;
	}

	/**
	 * @param uniqueId
	 *            the uniqueId to set
	 */
	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}

	/**
	 * @return the prefStationIdList
	 */
	public List<String> getPrefStationIdList() {
		return this.prefStationIdList;
	}

	/**
	 * @param prefStationIdList
	 *            the prefStationIdList to set
	 */
	public void setPrefStationIdList(List<String> prefStationIdList) {
		this.prefStationIdList = prefStationIdList;
	}
	
	/**
	 * @return the backStationIdList
	 */
	public List<String> getBackStationIdList() {
		return this.backStationIdList;
	}

	/**
	 * @param backStationIdList
	 *            the backStationIdList to set
	 */
	public void setBackStationIdList(List<String> backStationIdList) {
		this.backStationIdList = backStationIdList;
	}

	/**
	 * @return the isPrevPlanned
	 */
	public boolean isPrevPlanned() {
		return this.isPrevPlanned;
	}

	/**
	 * @param isPrevPlanned
	 *            the isPrevPlanned to set
	 */
	public void setPrevPlanned(boolean isPrevPlanned) {
		this.isPrevPlanned = isPrevPlanned;
	}

	/**
	 * @return the pRMode
	 */
	@Override
	public PRMode getPRMode() {
		return this.pRMode;
	}

	/**
	 * @param pRMode
	 *            the pRMode to set
	 */
	@Override
	public void setPRMode(PRMode pRMode) {
		this.pRMode = pRMode;
	}

	/**
	 * @return the bite
	 */
	public BITE getBite() {
		return bite;
	}

	/**
	 * @param bite
	 *            the bite to set
	 */
	public void setBite(BITE bite) {
		this.bite = bite;
	}

	/**
	 * @return the replDTOIdList
	 */
	public ArrayList<String> getReplDTOIdList() {
		return replDTOIdList;
	}

	/**
	 * @param replDTOIdList the replDTOIdList to set
	 */
	public void setReplDTOIdList(ArrayList<String> replDTOIdList) {
		this.replDTOIdList = replDTOIdList;
	}

	/**
	 * @return the isDecrBIC
	 */
	public boolean isDecrBIC() {
		return isDecrBIC;
	}

	/**
	 * @param isDecrBIC the isDecrBIC to set
	 */
	public void setDecrBIC(boolean isDecrBIC) {
		this.isDecrBIC = isDecrBIC;
	}

	/**
	 * @return the isTimePerf
	 */
	public boolean isTimePerf() {
		return isTimePerf;
	}

	/**
	 * @param isTimePerf the isTimePerf to set
	 */
	public void setTimePerf(boolean isTimePerf) {
		this.isTimePerf = isTimePerf;
	}
	
	
	

}
