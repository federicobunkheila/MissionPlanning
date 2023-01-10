/**
*
* MODULE FILE NAME: SessionChecker.java
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

package com.telespazio.csg.spla.csps.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.PRType;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.UserInfo;

public class RequestChecker {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(RequestChecker.class);

	
	/**
	 * Check if the given AR has equivalent DTOs 
	 *
	 * @param pSId
	 * @param schedARId
	 * @return
	 */
	public static boolean hasEquivDTO(Long pSId, String schedARId) {
	
		/**
		 * The EquivalentDTO boolean 
		 */
		boolean hasEquivDTO = false;
	
		if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(schedARId)) {
			
			if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId)
					.getEquivalentDTO() != null) {
	
				logger.trace("The scheduling AR " + schedARId + " has Equivalent DTO.");
	
				// Set true
				hasEquivDTO = true;
			}
		}
	
		return hasEquivDTO;
	}
	
	/**
	 * Check if the given AR has linked DTO 
	 *
	 * @param pSId
	 * @param schedDTOId
	 * @return
	 */
	public static boolean hasLinkedDTO(Long pSId, String schedDTOId) {
	
		/**
		 * The LinkedDTO boolean 
		 */
		boolean hasLinkedDTO = false;
	
		if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTOId)) {
			
			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId)
					.getLinkedDtoList() != null && ! PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId)
							.getLinkedDtoList().isEmpty()) {
	
				logger.trace("The DTO " + schedDTOId + " has linked DTO.");
	
				// Set true
				hasLinkedDTO = true;
			}
		}
	
		return hasLinkedDTO;
	}

	/**
	 * Return true if the start time of the Task is inside 
	 * the Mission Horizon of the given Planning Session Id
	 * // Created on 27/05/2022 for the management of Partially Inside MH Tasks
	 * @param pSId
	 * @param acq
	 */
	public static boolean isPartialInsideMH(Long pSId, Task task) {
			
		/**
		 * The inside boolean 
		 */
		boolean isInside = true;
		
		if ((task.getStopTime().getTime() <= SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime().getTime())
				|| (task.getStartTime().getTime() > SessionActivator.planSessionMap
						.get(pSId).getMissionHorizonStopTime().getTime())) {
			
			// Set inside
			isInside = false;
		}
		
		return isInside;
	}

	/**
	 * Return true if the start time of the DTO is inside the Mission Horizon of the given Planning Session Id
	 * @param pSId
	 * @param dto
	 */
	public static boolean isInsideMH(Long pSId, DTO dto) {
		
		/**
		 * The inside boolean
		 */
		boolean isInside = true;
		
		if ((dto.getStartTime().getTime() < SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime().getTime())
				|| (dto.getStartTime().getTime() > SessionActivator.planSessionMap
						.get(pSId).getMissionHorizonStopTime().getTime())) {
			
			// Set inside
			isInside = false;
		}
		
		return isInside;
	
	}

	/**
	 * Return true if the start time of the DTO is inside the Mission Horizon of the given Planning Session Id
	 * @param pSId
	 * @param schedDTO
	 */
	public static boolean isInsideMH(Long pSId, SchedDTO schedDTO) {
		
		/**
		 * The inside boolean 
		 */
		boolean isInside = true;
		
		if ((schedDTO.getStartTime().getTime() < SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime().getTime())
				|| (schedDTO.getStartTime().getTime() > SessionActivator.planSessionMap
						.get(pSId).getMissionHorizonStopTime().getTime())) {
			
			// Set inside
			isInside = false;
		}
		
		return isInside;
	}

	/**
	 * Return true if the start time of the Task is inside 
	 * the Mission Horizon of the given Planning Session Id
	 * @param pSId
	 * @param acq
	 */
	public static boolean isInsideMH(Long pSId, Task task) {
			
		/**
		 * The inside boolean 
		 */
		boolean isInside = true;
		
		if ((task.getStartTime().getTime() < SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime().getTime())
				|| (task.getStartTime().getTime() > SessionActivator.planSessionMap
						.get(pSId).getMissionHorizonStopTime().getTime())) {
			
			// Set inside
			isInside = false;
		}
		
		return isInside;
	}
	
	/**
	 * Return true if the start time of the Task is inside 
	 * the Mission Horizon of the given Planning Session Id
	 * @param pSId
	 * @param acq
	 */
	public static boolean isInsideMH(Long pSId, com.nais.spla.brm.library.main.ontology.tasks.Task task) {
			
		/**
		 * The inside boolean 
		 */
		boolean isInside = true;
		
		if ((task.getStartTime().getTime() < SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime().getTime())
				|| (task.getStartTime().getTime() > SessionActivator.planSessionMap
						.get(pSId).getMissionHorizonStopTime().getTime())) {
			
			// Set inside
			isInside = false;
		}
		
		return isInside;
	}
	
	/**
	 * Return true if the start time of the Task is inside 
	 * the previous Mission Horizon of the given Planning Session Id
	 * @param pSId
	 * @param startTime
	 */
	public static boolean isInsidePrevMH(Long pSId, Long startTime) {
			
		/**
		 * The inside boolean 
		 */
		boolean isInside = false;
		
		if ((startTime > (SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime().getTime() - 43200000))
			&& (startTime < SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStopTime().getTime() - 43200000)) {
			
			// Set inside
			isInside = true;
		}
		
		return isInside;
	}
	
	/**
	 * Return true if the start time of the Task is inside 
	 * 2 previous Mission Horizon of the given Planning Session Id
	 * @param pSId
	 * @param startTime
	 */
	public static boolean isInsideAntePrevMH(Long pSId, Long startTime) {
			
		/**
		 * The inside boolean 
		 */
		boolean isInside = false;
		
		if ((startTime > (SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime().getTime() - 86400000))
			&& (startTime < SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStopTime().getTime() - 86400000)) {
			
			// Set inside
			isInside = true;
		}
		
		return isInside;
	}

	/**
	 * Check the stereoscopic flag
	 * 
	 * @param pSId
	 * @param schedDTOId
	 */
	public static boolean isStereoscopicDTO(Long pSId, String schedDTOId) {
		
		/**
		 * The stereoscopic boolean
		 */
		boolean isStereo = false;
		
		if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(ObjectMapper.getSchedPRId(schedDTOId))) {
				
			if (PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(schedDTOId)).getIsStereopair()) {
				
				isStereo = true;
			}
		}
		
		return isStereo;
	}
	
	/**
	 * Check the interferometric flag
	 * 
	 * @param pSId
	 * @param schedDTOId
	 */
	public static boolean isInterferometricDTO(Long pSId, String schedDTOId) {
		
		/**
		 * The interferometric boolean
		 */
		boolean isInter = false;
		
		if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(ObjectMapper.getSchedPRId(schedDTOId))) {
				
			if (PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(schedDTOId)).getIsInterferometric()) {
				
				isInter = true;
			}
		}
		
		return isInter;
	}
	
	/**
	 * Check if the given DTO has already rejected linked DTOs 
	 * 
	 * @param pSId
	 * @param schedDTO
	 * @return
	 * @throws Exception 
	 */
	public static boolean isLinkDTORejected(Long pSId, SchedDTO schedDTO) throws Exception { 
		
		RulesPerformer rulesPerformer = new RulesPerformer();
		
		/**
		 * The rejection boolean
		 */
		boolean isLinkRejected = false;
		
		rulesPerformer.setRejectedDTOs(pSId);
		
		if (schedDTO.getLinkDtoIdList() != null && ! schedDTO.getLinkDtoIdList().isEmpty()) {
						
			if (! RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(
					schedDTO.getLinkDtoIdList().get(0))) {
				
				// Set linkedDTO
				isLinkRejected = false;
			}		
		}
		
		return isLinkRejected;	
	}
	
	/**
	 * Check if the given DTO has an already scheduled linked DTO 
	 * 
	 * @param pSId
	 * @param schedDTO
	 * @return
	 * @throws Exception 
	 */
	public static boolean isLinkDTOScheduled(Long pSId, SchedDTO schedDTO) throws Exception { 
				
		/**
		 * The scheduled boolean
		 */
		boolean isLinkScheduled = false;
		
		if (schedDTO != null && schedDTO.getLinkDtoIdList() != null 
				&& !schedDTO.getLinkDtoIdList().isEmpty()) {
							
			if (RulesPerformer.getPlannedDTOIds(pSId).contains(schedDTO.getLinkDtoIdList().get(0))) {
				
				// Set linkedDTO
				isLinkScheduled = true;
			}		
		}
		
		return isLinkScheduled;	
	}

	/**
	 * Return true if the PR visibility is International
	 *
	 * @param pRVisibility
	 * @return
	 */
	public static boolean isInternational(String pRVisibility) {
	
		/**
		 * The International boolean
		 */
		boolean isInter = false;
	
		if ((pRVisibility != null) && pRVisibility.equalsIgnoreCase("International")) {
	
			logger.trace("The PR is International.");
			
			// Set International
			isInter = true;
		}
	
		return isInter;
	}

	/**
	 * Return true if the PR visibility is National
	 *
	 * @param pRVisibility
	 * @return
	 */
	public static boolean isNational(String pRVisibility) {
	
		/**
		 * The National boolean
		 */
		boolean isNat = false;
	
		if ((pRVisibility != null) && pRVisibility.equalsIgnoreCase("National")) {
	
			// Set National		
			isNat = true;
		}
	
		return isNat;
	}

	/**
	 * Return true if the PR visibility is NEO
	 *
	 * @param pRVisibility
	 * @return
	 */
	public static boolean isNEO(String pRVisibility) {
	
		/**
		 * The NEO boolean
		 */
		boolean isNEO = false;
	
		if ((pRVisibility != null) && pRVisibility.equalsIgnoreCase("NEO")) {
	
			// Set NEO
			isNEO = true;
		}
	
		return isNEO;
	}

	/**
	 * Return true if the request related to the task is NEO
	 *
	 * @param pSId
	 * @param acq
	 * @return
	 */
	public static boolean isNEOTask(Long pSId, Task acq) {
	
		/**
		 * The scheduling PR Id
		 */
		String schedPRId = ObjectMapper.parseDMToSchedPRId(
				acq.getUgsId(), acq.getProgrammingRequestId());
		
		if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedPRId)) {

			if (PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getVisibility().equals("NEO")) {
	
				// Return true
				return true;

			} else {

				return false;
			}
		}
	
		return false;
	}

	/**
	 * Return true if the PR visibility is NotNEO
	 *
	 * @param pRVisibility
	 * @return
	 */
	public static boolean isNotNEO(String pRVisibility) {
	
		/**
		 * The NotNEO boolean
		 */
		boolean isNotNEO = false;
	
		if ((pRVisibility != null) && pRVisibility.equalsIgnoreCase("NotNEO")) {
	
			// Set NotNEO			
			isNotNEO = true;
		}
	
		return isNotNEO;
	}
	
	/**
	 * Return true if the PR is for Defence Partner
	 *
	 * @param ugsId
	 * @return
	 */
	public static boolean isDefence(String ugsId) {
	
		/**
		 * The Defence boolean
		 */
		boolean isDef = false;
	
		if (ugsId.startsWith("2")) {
	
			logger.trace("The PR is Defene.");
			
			// Set International
			isDef = true;
		}
	
		return isDef;
	}

	/**
	 * Check if the given AR is Standard mode DTOs
	 *
	 * @param pSId
	 * @param schedARId
	 * @return
	 */
	public static boolean isStandardAR(Long pSId, String schedARId) {
	
		/**
		 * The Standard boolean
		 */
		boolean isStandard = false;
	
		if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(schedARId)) {
			
			if (PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(schedARId))
					.getMode().equals(PRMode.Standard)) {
	
				// Set Standard	
				isStandard = true;
			}
		}
	
		return isStandard;
	}

	/**
	 * Return true if the PR is LMP type, false otherwise
	 *
	 * @param schedPRId
	 * @return
	 */
	public static boolean isCrisis(PRType pRType) {
	
		/**
		 * The Crisis boolean
		 */
		boolean isCrisis = false;
	
		if (pRType!= null && (pRType.equals(PRType.CRISIS_HP) 
				|| pRType.equals(PRType.CRISIS_PP) || pRType.equals(PRType.CRISIS_ROUTINE))) {
	
			// Set Crisis
			isCrisis = true;				
		}
	
		return isCrisis;
	}
	
	/**
	 * Return true if the PR is a replacing civilian, false otherwise
	 *
	 * @param schedPRId
	 * @return
	 */
	public static boolean isReplacing(ProgrammingRequest pR) {
	
		/**
		 * The replacing boolean
		 */
		boolean isReplacing = false;
	
		if (pR.getReplacingCivilianRequestFlag() != null && pR.getReplacingCivilianRequestFlag()) {
	
			// Set replacing
			isReplacing = true;
			
			logger.trace("The request is a replacing civilian.");	
					
		}
	
		return isReplacing;
	}
	
	/**
	 * Return true if the PR is a replacing civilian, false otherwise
	 *
	 * @param pR
	 * @return
	 */
	public static boolean isSubscribed(ProgrammingRequest pR) {
	
		/**
		 * The subscribed boolean
		 */
		boolean isSubscriber = false;
	
		for (UserInfo userInfo : pR.getUserList()) {
	
			if (userInfo.isSubscriber().equals(true)) {
				
				isSubscriber = true;
				
				logger.trace("The request is subscribed.");	
			}
					
		}
	
		return isSubscriber;
	}
	
	/**
	 * Return true if the PR has a civilian unique Id, false otherwise
	 *
	 * @param schedPRId
	 * @return
	 */
	public static boolean hasUniqueId(ProgrammingRequest pR) {
	
		/**
		 * The uniqueId boolean
		 */
		boolean hasUniqueId = false;
	
		if (pR.getCivilianUniqueId() == null) {
	
			// Set uniqueId
			hasUniqueId = true;
					
		} else {
			
			logger.trace("The request has a civilian unique Id.");	
		}
	
		return hasUniqueId;
	}

	/**
	 * Return true if the PR is LMP type, false otherwise
	 *
	 * @param schedPRId
	 * @return
	 */
	public static boolean isLMP(PRType pRType) {
	
		/**
		 * The LMP boolean
		 */
		boolean isLMP = false;
	
		if (pRType != null && (pRType.equals(PRType.LMP_HP) 
				|| pRType.equals(PRType.LMP_PP) || pRType.equals(PRType.LMP_ROUTINE))) {
			
			// Set LMP
			isLMP = true;
			
			logger.trace("The request is of LMP type.");		
		}
	
		return isLMP;
	}

	/**
	 * Return true if the PR is VU type, false otherwise
	 *
	 * @param schedPRId
	 * @return
	 */
	public static boolean isVU(PRType pRType) {
	
		/**
		 * The VU boolean
		 */
		boolean isVU = false;
	
		if (pRType!= null && (pRType.equals(PRType.VU_HP) 
				|| pRType.equals(PRType.VU_PP) || pRType.equals(PRType.VU_ROUTINE))) {
	
			// Set VU
			isVU = true;
	
			
			logger.trace("The request is of VU type.");
		}
	
		return isVU;
	}
	
	/**
	 * Check if single polarization H
	 * @param polar
	 * @return
	 */
	public static boolean isSinglePolarH(String polar) throws Exception {

		switch (polar) {

		case "HH":
			return true;
		case "HH+VH":
			return true;
		case "HH/HH":
			return true;
		case "HH/HH+VH/VH":
			return true;
		case "HH/VH+HH/VH":
			return true;
			
		default:
			return false;
		}
	}
	
	/**
	 * Check if single polarization V
	 * @param polar
	 * @return
	 */
	public static boolean isSinglePolarV(String polar) throws Exception {

		switch (polar) {

		case "VV":
			return true;
		case "VV+HV":
			return true;
		case "VV/VV":
			return true;
		case "VV/HV+VV/HV":
			return true;
		case "VV/VV+HV/HV":
			return true;
				
		default:
			return false;
		}
	}
	
	/**
	 * Check if dual polarization H and V
	 * @param polar
	 * @return
	 */
	public static boolean isDualPolar(String polar) throws Exception {

		/**
		 * The dual polarization boolean
		 */
		boolean isDualPol = false;
		
		if (!isSinglePolarH(polar) && !isSinglePolarV(polar)) {
			
			isDualPol = true;
		}
				
		return isDualPol;
	}

}
