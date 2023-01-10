/**
 *
 * MODULE FILE NAME: ConflictRankCalculator.java
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.drools.DroolsQueries;
import com.nais.spla.brm.library.main.ontology.enums.ReasonOfReject;
import com.nais.spla.brm.library.main.ontology.resources.ReasonOfRejectElement;
import com.nais.spla.brm.library.main.ontology.tasks.Acquisition;
import com.nais.spla.brm.library.main.ontology.utils.ElementsInvolvedOnOrbit;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;

import it.sistematica.spla.datamodel.core.enums.DtoStatus;

/**
 * The conflict DTO calculator
 * 
 * @author bunkheila
 *
 */
public class ConflictDTOCalculator {

	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(ConflictDTOCalculator.class);

	/**
	 * 
	 * @param pSId
	 * @param rejDTOId
	 * @param reason
	 * @return
	 */
	public Integer getConflictRank(Long pSId, String rejDTOId, ReasonOfReject reason)
	{    
		
		// Initialize the conflict rank
		Integer conflRank = null;

		// Initialize the type of rank
		boolean rankUp = true;

		// Initialize the relative acq
		Acquisition acq = null;

		// get all rejected elements
		Map<String, Acquisition> rejAcqMap = RulesPerformer.brmOperMap.get(pSId)
				.receiveDtoRejected(pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId));

		// check if the id given as input is of a rejected acq
		try
		{
			// get the acq
			acq = rejAcqMap.get(rejDTOId);

			// check if the reason of reject of the acquisition match with one
			// of the reasons that need a rankUp
			boolean up = reason.equals(ReasonOfReject.acqOverlapWithAcquisition) || reason.equals(ReasonOfReject.maneuverOverlapDloOrPaw)
					|| reason.equals(ReasonOfReject.maneuverOverlapWithAcquisition) || reason.equals(ReasonOfReject.noTimeForAxesReconf)
					|| reason.equals(ReasonOfReject.acqOverlapWithPassThrough) || reason.equals(ReasonOfReject.passThroughOverlapAcq)
					|| reason.equals(ReasonOfReject.acqOverlapWithAcquisition);

			// check if the reason of reject of the acquisition match with one
			// of the reasons that need a rankDown
			boolean down = reason.equals(ReasonOfReject.minDistanceViolation) || reason.equals(ReasonOfReject.noTimeForAManeuver)
					|| reason.equals(ReasonOfReject.acqOverlapTheatre) || reason.equals(ReasonOfReject.smoothingEssUnranked)
					|| reason.equals(ReasonOfReject.noSpaceInPdht) || reason.equals(ReasonOfReject.HpFixedOrbitLimitReached)
					|| reason.equals(ReasonOfReject.maxNumberPrType) || reason.equals(ReasonOfReject.noEnergyForSilentInEclipse)
					|| reason.equals(ReasonOfReject.noEnergyForSilent) || reason.equals(ReasonOfReject.reachedOrbitThreshold)
					|| reason.equals(ReasonOfReject.reachedOrbitThresholdInEclipse) || reason.equals(ReasonOfReject.CMGFormulaViolation)
					|| reason.equals(ReasonOfReject.leftAttitudeModeViolation) || reason.equals(ReasonOfReject.essLeftMoreThanThreshold)
					|| reason.equals(ReasonOfReject.moreThanLeftAttitudeTimeThreshold) || reason.equals(ReasonOfReject.moreThanUpperBoundPeaksBic)
					|| reason.equals(ReasonOfReject.reachedOrbitThreshold);

			// if the requested reason is up
			if (acq != null && up) {
				
				logger.info("Compute conflict rank up for rejected DTO: " + rejDTOId);
				conflRank = rankUpDown(pSId, acq, rankUp);

			// if the requested reason is down
			} else if (acq != null && down) {
				
				logger.info("Compute conflict rank down for rejected DTO: " + rejDTOId);
				conflRank = rankUpDown(pSId, acq, rankUp);
				rankUp = false;

			} else {
				
				// no rank is returned
				logger.debug("No conflict rank value has to be returned for rejected DTO: " + rejDTOId);
			}

		}
		catch (Exception ex) {
			
			logger.error("Exception raised during the conflict rank computation: " + ex.getStackTrace()[0].toString());
		}
		
		return conflRank;
	}

	/**
	 * Get the upper rank of all acquisitions involved in rejection for the given partner.
	 * @param pSId
	 * @param acq
	 * @param rankUp
	 * @return
	 */
	public Integer rankUpDown(Long pSId, Acquisition acq, boolean rankUp)
	{

		//initialize the selected rank to null
		String selDTOId = null;

		//get all the acquisitions involved in reject of current acq
		List<String> acqInvolved = getAcqsInvolvedInReject(acq);

		//get the acquisitions related to partner
		List<Acquisition> partnerAcqList = getPartnerAcqs(pSId, acq, acqInvolved);
		logger.debug("list of acquisition involved in reject :" +partnerAcqList);

		//if there are acquisitions in overlap
		if (partnerAcqList != null && !partnerAcqList.isEmpty())
		{
			//sort acquisitions by priority
			sortAcqByPriority(partnerAcqList);
			logger.debug("list of acquisition ordered by priority :" +partnerAcqList);

			//if is requested a rank up
			if (rankUp)
			{
				//get the highest priority element
				selDTOId = partnerAcqList.get(0).getIdTask();
				logger.debug("get the highest rank acq :" +selDTOId);
			}
			//if is requested a rank down
			else
			{
				//get the lowest priority element
				selDTOId = partnerAcqList.get(partnerAcqList.size() - 1).getIdTask();
				logger.debug("get the lowest rank acq :" +selDTOId);
			}
		}
		
		if (selDTOId != null) {
		
			// Return the rank of the selected DTO Id
			return getSchedDTOIdRank(pSId, selDTOId);
		
		}
		
		return null;
	}

	/**
	 * Get the all acqs involved in reject.
	 *
	 * @param acq the acq
	 * @return the all acqs involved in reject
	 */
	private List<String> getAcqsInvolvedInReject(Acquisition acq)
	{
		//create an empty list for the id of elements involved
		List<String> acqInvolved = new ArrayList<String>();

		if (acq.getReasonOfReject() != null) {
		
			//iterate over the reason of reject linked with the acquisition
			for (int i = 0; i < acq.getReasonOfReject().size(); i++)
			{
				//if there are elements involved
				if (acq.getReasonOfReject().get(i).getElementsInvolved() != null)
				{
					//iterate over the map of involved elements on orbit (the orbit is the key of the map)
					for (Map.Entry<Double, List<ElementsInvolvedOnOrbit>> elementsInvolvedIterator : acq.getReasonOfReject().get(i).getElementsInvolved().entrySet())
					{
						List<ElementsInvolvedOnOrbit> elementsInvolvedOnOrbit = elementsInvolvedIterator.getValue();
	
						//iterate over the sliding window impacted in reject cause
						for (int j = 0; j < elementsInvolvedOnOrbit.size(); j++)
						{
							//for each sliding window extract the elements in overlap
							List<String> potentialAcqInvolved = elementsInvolvedOnOrbit.get(j).getElementsInvolved();
	
							//invoke the function to add the id at the list of elements involved
							addAcquisition(acqInvolved, potentialAcqInvolved);
						}
					}
				}
			}
		}
		
		return acqInvolved;
	}

	/**
	 * Add the acquisition.
	 *
	 * @param acqInvolved the acq involved
	 * @param potentialAcqInvolved the potential acq involved
	 */
	private void addAcquisition(List<String> acqInvolved, List<String> potentialAcqInvolved) {
	
		// Check potential acquisition
		if (potentialAcqInvolved!=null && !potentialAcqInvolved.isEmpty()) {
		
			for (int i = 0; i < potentialAcqInvolved.size(); i++)
			{
				if (!(acqInvolved.contains((potentialAcqInvolved.get(i)))))
				{
					// Add involved acquisition
					acqInvolved.add(potentialAcqInvolved.get(i));
				}
			}
		}        
	}

	/**
	 * Sort acq by priority.
	 *
	 * @param acqList the acq list
	 */
	private void sortAcqByPriority(List<Acquisition> acqList)
	{
		// Sort acquisition list
		Collections.sort(acqList, new Comparator<Acquisition>()
		{

			@Override
			// given two tasks
			public int compare(Acquisition acq1, Acquisition acq2)
			{
				// compare and sort them by startTime
				return acq1.getPriority() - (acq2.getPriority());
			}
		});
	}

	/**
	 * Gets the only acquisitions related to partner.
	 *
	 * @param pSId
	 * @param acq the acq
	 * @param acqInvolved the acq involved
	 * @return the only acquisitions related to partner
	 */
	private List<Acquisition> getPartnerAcqs(Long pSId, Acquisition acq, List<String> acqInvolved)
	{
		
		DroolsQueries droolsQueries = new DroolsQueries();
		
		// create an empty list of acquisitions
		List<Acquisition> conflAcqList = new ArrayList<Acquisition>();

//		// create an instance of DroolsQueries
//		DroolsQueries droolsQueries = new DroolsQueries();

		// get all the id of the partners involved with the rejected acquisition
		List<String> partnerRejDTOIdList = getPartnerInvolved(acq);

		// iterate over the acquisitions involved in the cause of reject
		for (int i = 0; i < acqInvolved.size(); i++)
		{
			// get the acquisition that is referred with the extracted id
			Acquisition relatedAcq = droolsQueries.getAcqWithId(pSId.toString(), 
					RulesPerformer.brmInstanceMap.get(pSId),acqInvolved.get(i));

			// if there is an associated acq
			if (relatedAcq != null)
			{
//				// get all the id of the partners involved with this acquisition
//				List<String> allPartnersWithCurrentAcq = getPartnerInvolved(relatedAcq);

				// iterate over the partners involved in the rejected one
				for (int j = 0; j < partnerRejDTOIdList.size(); j++)
				{
//					// if the partner's list of the current acquisition contains
//					// at least one partner of the rejected one
//					if (allPartnersWithCurrentAcq.contains(partnerRejDTOIdList.get(j)))
//					{
						// add the current acq to the list of valid acq
						if(!conflAcqList.contains(relatedAcq))
						{
							conflAcqList.add(relatedAcq);
						}
//					}
				}
			}
		}
		return conflAcqList;
	}

	/**
	 * Gets the partner involved.
	 *
	 * @param acq
	 *            the acq
	 * @return the partner involved
	 */
	public List<String> getPartnerInvolved(Acquisition acq) {
		// initialize an empty list of string that will contains all the
		// partners id involved with the acq
		List<String> partnerIdList = new ArrayList<String>();

		// iterate over the userInfo linked to the acq
		for (int i = 0; i < acq.getUserInfo().size(); i++) {
			// add the id of the partner
			partnerIdList.add(acq.getUserInfo().get(i).getOwnerId());
		}
		return partnerIdList;
	}

	/**
	 * Check if it is a binary reason of rejection.
	 * @param reason
	 * @return
	 */
	public static boolean isBinaryReason(ReasonOfReject reason) {

		/**
		 * The binary boolean
		 */
		boolean isBinary = false;

		// Check reason of rejection
		if (reason.equals(ReasonOfReject.acqOverlapWithAcquisition)
				|| reason.equals(ReasonOfReject.acqOverlapWithPassThrough) 
				|| reason.equals(ReasonOfReject.passThroughOverlapAcq)
				|| reason.equals(ReasonOfReject.theatreOverlapAcquisition)
				|| reason.equals(ReasonOfReject.acqOverlapTheatre)
				|| reason.equals(ReasonOfReject.noTimeForAManeuver)
				|| reason.equals(ReasonOfReject.maneuverOverlapWithAcquisition)
				|| reason.equals(ReasonOfReject.maneuverOverlapDloOrPaw)				
				|| reason.equals(ReasonOfReject.noTimeForAxesReconf)
				|| reason.equals(ReasonOfReject.minDistanceViolation)) {

			// Set binary	
			isBinary = true;
		}

		return isBinary;		
	}
	
	/**
	 * Check if the overlap is conflicting for CBJ
	 *
	 * @param reason
	 */
	public static boolean isCBJConflictReason(ReasonOfReject reason) {

		/**
		 * The conflict boolean
		 */
		boolean isConflict = false;

		// Check reason of rejection
		if (reason.equals(ReasonOfReject.acqOverlapWithAcquisition)
				|| reason.equals(ReasonOfReject.maneuverOverlapWithAcquisition)
				|| reason.equals(ReasonOfReject.theatreOverlapAcquisition)
				|| reason.equals(ReasonOfReject.acqOverlapWithPassThrough)
				|| reason.equals(ReasonOfReject.passThroughOverlapAcq)
				|| reason.equals(ReasonOfReject.minDistanceViolation)
				|| reason.equals(ReasonOfReject.noTimeForAManeuver)
				|| reason.equals(ReasonOfReject.CMGFormulaViolation)
				|| reason.equals(ReasonOfReject.noTimeForAxesReconf)
				|| reason.equals(ReasonOfReject.essLeftMoreThanThreshold)
				|| reason.equals(ReasonOfReject.maxPeaksReached)
				|| reason.equals(ReasonOfReject.HpFixedOrbitLimitReached)
				|| reason.equals(ReasonOfReject.leftAttitudeModeViolation)
				|| reason.equals(ReasonOfReject.moreThanUpperBoundPeaksBic)
				|| reason.equals(ReasonOfReject.noEnergyForSilent)
				|| reason.equals(ReasonOfReject.noEnergyForSilentInEclipse)
				|| reason.equals(ReasonOfReject.reachedOrbitThreshold)			
				|| reason.equals(ReasonOfReject.reachedOrbitThresholdInEclipse))
//			    || reason.equals(ReasonOfReject.noSpaceInPdht)
//				|| reason.equals(ReasonOfReject.acqOverlapTheatre) 
//				|| reason.equals(ReasonOfReject.maxNumberPrType)
		{
			// Set conflict
			isConflict = true;
		}

		return isConflict;
	}

	/**
	 * Check if the overlap is conflicting
	 *
	 * @param reason
	 */
	public static boolean isConflictReason(ReasonOfReject reason) {

		/**
		 * The conflict boolean
		 */
		boolean isConflict = false;

	
		// Check reason of rejection
		if (reason.equals(ReasonOfReject.acqOverlapWithAcquisition)
				|| reason.equals(ReasonOfReject.maneuverOverlapWithAcquisition)
				|| reason.equals(ReasonOfReject.acqOverlapWithPassThrough)
				|| reason.equals(ReasonOfReject.acqOverlapTheatre) 
				|| reason.equals(ReasonOfReject.passThroughOverlapAcq)
				|| reason.equals(ReasonOfReject.minDistanceViolation)
				|| reason.equals(ReasonOfReject.noTimeForAManeuver)
				|| reason.equals(ReasonOfReject.noTimeForAxesReconf)
				|| reason.equals(ReasonOfReject.CMGFormulaViolation)
				|| reason.equals(ReasonOfReject.essLeftMoreThanThreshold)
				|| reason.equals(ReasonOfReject.maxPeaksReached)
				|| reason.equals(ReasonOfReject.HpFixedOrbitLimitReached)
				|| reason.equals(ReasonOfReject.leftAttitudeModeViolation)
				|| reason.equals(ReasonOfReject.maxNumberPrType)
				|| reason.equals(ReasonOfReject.moreThanUpperBoundPeaksBic)
				|| reason.equals(ReasonOfReject.noEnergyForSilent)
				|| reason.equals(ReasonOfReject.noEnergyForSilentInEclipse)
				|| reason.equals(ReasonOfReject.noSpaceInPdht)
				|| reason.equals(ReasonOfReject.reachedOrbitThreshold)			
				|| reason.equals(ReasonOfReject.reachedOrbitThresholdInEclipse))
		{
			// Set conflict
			isConflict = true;
		}

		return isConflict;
	}
		
	/**
	 * Get the rank of a given scheduling DTO Id
	 * @param pSId
	 * @param conflDTOId
	 * @return
	 */
	private Integer getSchedDTOIdRank(Long pSId, String schedDTOId) {
			
		/**
		 * The output rank
		 */
		Integer rank = null;
		
		try {
			
			/**
			 * The scheduling AR Id
			 */
			String schedARId = ObjectMapper.getSchedARId(schedDTOId);
			
			if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(schedARId)) {
				
				// Set rank				
				rank = PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getRank();
			
			} else {
			
				logger.warn("No DTO rank is found!");
			}
			
		} catch (Exception ex) {
			
			logger.error(ex.getStackTrace()[0].toString());
		}
		
		return rank;
	}
	
	
	/**
	 * Order DTOs by worth
	 * 
	 * @param pSId
	 * @param pR
	 * @param orderDTOList
	 * @return
	 */
	public void orderWorthDTOs(Long pSId, ArrayList<SchedDTO> orderDTOList) {
		
		logger.debug("Sort DTO list according to the worth priorities.");

		// Order  DTOs by worth
		if (orderDTOList.size() > 0) {

			// Filter overlapping DTOs 
			ConflictDTOCalculator.filterOverlapDTOs(pSId, orderDTOList);
			
			if (!SessionChecker.isUnranked(pSId) && !SessionChecker.isSelf(pSId) 
				&& !RequestChecker.isStereoscopicDTO(pSId, orderDTOList.get(0).getDTOId()) 
				&& !RequestChecker.isInterferometricDTO(pSId, orderDTOList.get(0).getDTOId())) {

				Collections.sort(orderDTOList, new DTOWorthComparator());
			} 
			else {
				
				Collections.sort(orderDTOList, new Comparator<SchedDTO>() {

					/**
					 * Compare linked DTOs
					 * 
					 * @param x
					 * @param y
					 */
					@Override
					public int compare(SchedDTO dto1, SchedDTO dto2) {

						/**
						 * The first DTO link
						 */
						double link1 = 0;
						
						if (dto1.getLinkDtoIdList() != null && !dto1.getLinkDtoIdList().isEmpty()) {

							if (RulesPerformer.getPlannedDTOIds(pSId).contains(dto1.getLinkDtoIdList().get(0))) {
								link1 = 1;
							}
						}
						
						/**
						 * The second DTO link
						 */
						double link2 = 0;

						if (dto2.getLinkDtoIdList() != null && !dto2.getLinkDtoIdList().isEmpty()) {

							if (RulesPerformer.getPlannedDTOIds(pSId).contains(dto2.getLinkDtoIdList().get(0))) {
								link2 = 1;
							}
						}
						
						/**
						 * The comparing value
						 */
						int compVal = Double.compare(link1, link2);

						return compVal;
					}
				});
			}
		}
	}
	
	/**
	 * Filter for arc-consistency a new AR DTOs sorted by minimum target distance
	 * 
	 * @param pSId
	 * @param schedDTOList
	 * @return
	 */
	public static ArrayList<SchedDTO> filterOverlapDTOs(Long pSId, ArrayList<SchedDTO> schedDTOList) {

		logger.debug("Filter overlapping DTOs relevant to AR: " + schedDTOList.get(0).getARId());
		
		// Sort DTOs by target distance
		Collections.sort(schedDTOList, new SchedDTODistComparator());

		// Filter new DTOs
		for (SchedDTO schedDTO : schedDTOList) {

			for (SchedDTO checkDTO : schedDTOList) {

				if (schedDTO.getDTOId() != checkDTO.getDTOId()) {

					if (isOverlapped(pSId, checkDTO, schedDTO)) {

						// Update DTO status
						checkDTO.setStatus(DtoStatus.Rejected);

						break;
					}
				}
			}
		}

		return schedDTOList;
	}

	/**
	 * Check if a DTO is overlapped by another one for arc-inconsistency.
	 *
	 * @param checkDTO
	 *            - the DTO to be checked
	 * @param schedDTO
	 *            - the scheduled DTO
	 * @return the overlapping boolean
	 */
	private static boolean isOverlapped(Long pSId, SchedDTO checkDTO, SchedDTO schedDTO) {

		/**
		 * The output boolean
		 */
		boolean isOverlapped = false;

		// Check time intervals
		if (((checkDTO.getStartTime().getTime() >= schedDTO.getStartTime().getTime())
				&& (checkDTO.getStartTime().getTime() <= schedDTO.getStopTime().getTime()))
				|| ((checkDTO.getStopTime().getTime() >= schedDTO.getStartTime().getTime())
						&& (checkDTO.getStopTime().getTime() <= schedDTO.getStopTime().getTime()))
				|| ((checkDTO.getStartTime().getTime() <= schedDTO.getStartTime().getTime())
						&& (checkDTO.getStopTime().getTime() >= schedDTO.getStopTime().getTime()))
				|| ((checkDTO.getStartTime().getTime() >= schedDTO.getStartTime().getTime())
						&& (checkDTO.getStopTime().getTime() <= schedDTO.getStopTime().getTime()))) {

			// Check target distance			
			if (checkDTO.getTargetDistance() > schedDTO.getTargetDistance()) {

				logger.debug("DTO with Id " + checkDTO.getDTOId() + " is a-priori rejected because overlapped by "
						+ schedDTO.getDTOId());

				/**
				 * The rejected reason element
				 */	
				ReasonOfRejectElement rejReasonEl = new ReasonOfRejectElement();
				rejReasonEl.setReason(ReasonOfReject.acqOverlapWithAcquisition);
				ElementsInvolvedOnOrbit invEl = new ElementsInvolvedOnOrbit();
				invEl.setElementsInvolved(Arrays.asList(schedDTO.getDTOId()));
				rejReasonEl.addElementInvolved(1, invEl);
				rejReasonEl.setDescription("Arc-inconsistent: overlap with DTO of same AR.");
				rejReasonEl.setId(7);
				
				SessionScheduler.rejDTOIdListMap.get(pSId).add(checkDTO.getDTOId());
				RulesPerformer.rejDTORuleListMap.get(pSId).put(checkDTO.getDTOId(), Arrays.asList(rejReasonEl));
				
				// Update overlap		
				isOverlapped = true;
			}
		}

		return isOverlapped;
	}

}
