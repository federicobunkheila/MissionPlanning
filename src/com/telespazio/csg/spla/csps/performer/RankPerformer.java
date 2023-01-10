/**
 *
 * MODULE FILE NAME: RankPerformer.java
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

package com.telespazio.csg.spla.csps.performer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.ontology.enums.ReasonOfReject;
import com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO;
import com.nais.spla.brm.library.main.ontology.resources.ReasonOfRejectElement;
import com.nais.spla.brm.library.main.ontology.utils.ElementsInvolvedOnOrbit;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.processor.ManualPlanProcessor;
import com.telespazio.csg.spla.csps.processor.NextARProcessor;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.utils.SchedDTODeltaTimeComparator;
import com.telespazio.csg.spla.csps.utils.SchedDTOSizeComparator;
import com.telespazio.csg.spla.csps.utils.ConflictDTOCalculator;
import com.telespazio.csg.spla.csps.utils.IntMatrixCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.ValueComparator;

/**
 * The Ranked-based scheduling performer class for the ARs DTOs through
 * heuristic algorithms.
 */
public class RankPerformer {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(RankPerformer.class);

	/**
	 * The complete set of the AR DTO Lists
	 */
	public static Map<Long, ArrayList<ArrayList<SchedDTO>>> schedDTODomainMap;

	/**
	 * The iteration index map
	 */
	public static HashMap<Long, Integer> iterMap;
	
	/**
	 * The jump counter map
	 */
	public static HashMap<Long, Integer> jumpMap;
	
	/**
	 * The map of the initially rejected DTOs list
	 */
	private static HashMap<String, List<ReasonOfRejectElement>> initRejDTOIdListMap;

	/**
	 * The list of the AR DTOs in the session
	 */
	private static ArrayList<SchedDTO> schedSol;

	/**
	 * The list of the AR Ids in the session
	 */
	private static ArrayList<String> schedARIdList;
	
	/**
	 * The initial list of the AR DTOs in the session
	 */
	private static ArrayList<SchedDTO> initSchedSol;

	/**
	 * The updated list of AR DTOs
	 */
	private ArrayList<SchedDTO> newSchedARDTOList;

	/**
	 * The map of the conflicting DTO Ids list
	 */
	public static HashMap<String, ArrayList<String>> conflDTOIdListMap;

	/**
	 * The map of the conflicting element list
	 */
	public static HashMap<String, List<ElementsInvolvedOnOrbit>> conflElementListMap;

	/**
	 * The map of the reason conflicting DTO Ids list
	 */
	public static HashMap<ReasonOfReject, ArrayList<String>> conflReasonDTOIdListMap;

	/**
	 * The map of the minimum AR download time
	 */
	public static HashMap<Long, Long> minARDwlTimeMap;

	/**
	 * The map of selected ARs indices
	 */
	private static HashMap<String, Integer> selARIndMap = new HashMap<>();
	
	/**
	 * The map of selected ARs indices
	 */
	private static HashMap<String, Integer> initSelARIndMap = new HashMap<>();
	
	/**
	 * The exit boolean
	 */
	HashMap<Long, Boolean> isExitMap = new HashMap<>();

	/**
	 * Perform the revised CBJ algorithm
	 * // TODO: test linked DTO management

 	 * @param pSId
	 *            - the current planning session Id
	 * @param aRDTOList
	 *            - the AR DTO list
	 * @param initSchedDomain
	 *            - the domain of previously scheduled AR DTO List
	 * @param initSchedSol
	 *            - the list of previously scheduled DTOs
	 * @param cutOffTime
	 *            - the cutoff time for the CBJ scheduling of the AR
	 * @return the revised CBJ solution
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public synchronized ArrayList<SchedDTO> performRevCBJ(Long pSId, ArrayList<SchedDTO> aRDTOList,
			ArrayList<ArrayList<SchedDTO>> initSchedDomain, ArrayList<SchedDTO> initSchedSol, 
			ArrayList<String> initSchedARIdList, double cutOffTime) throws Exception {

		try {

			/**
			 * The first guess boolean
			 */
			boolean firstGuess = true;

			logger.info("Perform conflict resolution for the AR: " + aRDTOList.get(0).getARId());
			
			// Initialize CBJ data
			initCBJData(pSId, initSchedDomain, initSchedSol, initSchedARIdList, aRDTOList);

			/**
			 * The DTO domain size
			 */
			int domainSize = schedDTODomainMap.get(pSId).size();

			if (! RequestChecker.hasEquivDTO(pSId, aRDTOList.get(0).getARId())) {
				
				// Check arc-consistency of the standard AR DTOs
				checkArcConsistency(pSId, domainSize);
			}
					
			logger.info("Potential size of the scheduled AR domain is: " + domainSize);

			/**
			 * Process counters
			 */
			int varCount = domainSize - 1;
			int maxCount = varCount;
			ArrayList<Integer> varCountList = new ArrayList<>();
			varCountList.add(maxCount);

			/**
			 * The var iteration counter map
			 */
			HashMap<Integer, Integer> iterCountMap = new HashMap<Integer, Integer>();
			
			iterCountMap.put(varCount, 0);
			
			// The conflict counter
			int conflCountIter = 0;
			
			/**
			 * The initial date
			 */
			Date initDate = new Date();

			logger.info("Available cut-off time (ms): " + cutOffTime);

			// While maximum iteration counter is not reached
			// TODO: erase (conflCountIter == 0)
			while ((varCount >= 0) && (varCount <= maxCount) 
					&& (schedARIdList.size() < domainSize)
					&& ((new Date().getTime() - initDate.getTime()) < cutOffTime)) {

				// Check exit condition
				if (!firstGuess && (varCount == maxCount) 
						&& checkCBJExitCondition(pSId, domainSize, varCount, varCountList)) {

					if (! RulesPerformer.checkARIdScheduling(pSId, 
							schedDTODomainMap.get(pSId).get(varCount).get(0).getARId())) {
												
						// Get the CBJ DTO to be scheduled at iteration
						getIterDTO(pSId, firstGuess, domainSize, varCount, varCountList);
					}
					
					break;
				}
				
				/**
				 * The DTO index
				 */
				int dtoInd = 0;

				// Check the DTO consistency
				if (getIterDTO(pSId, firstGuess, domainSize, varCount, varCountList) != null) {

					firstGuess = false;
					
					jumpMap.put(pSId, jumpMap.get(pSId) + 1);
					
					try {

						varCount = varCountList.get(varCountList.size() - 1);

					} catch (Exception ex) {

						logger.warn("Processing inconsistency raised.");

						continue;
					}

					// Update new CBJ list of DTOs
					if (varCount < domainSize && !RequestChecker.hasEquivDTO(
							pSId, schedDTODomainMap.get(pSId).get(varCount).get(0).getARId())
							&& !RequestChecker.isLinkDTOScheduled(pSId, schedDTODomainMap.get(pSId).get(varCount).get(0))
							&& !checkCBJExitCondition(pSId, domainSize, varCount, varCountList)) {

						logger.trace("Update new CBJ DTO list with AR: " + schedDTODomainMap.get(pSId).get(varCount).get(0).getARId());
						
						dtoInd = selARIndMap.get(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId());

						newSchedARDTOList.clear();
						newSchedARDTOList.add(schedDTODomainMap.get(pSId).get(varCount).get(dtoInd));

					} else {

						varCount = domainSize - 1;
						newSchedARDTOList = (ArrayList<SchedDTO>) aRDTOList.clone();
					}

				// Analyze inconsistent DTO
				} else {
					
					if (firstGuess) {
					
						dtoInd = selARIndMap.get(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId());
					}

					if (!varCountList.contains(varCount)) {

						varCountList.add(varCount);
					}

					// Iterate CBJ
					conflCountIter = iterCBJ(pSId, varCountList, varCount, iterCountMap, conflCountIter, 
							dtoInd, cutOffTime - (new Date().getTime() - initDate.getTime()));
					

					firstGuess = false;

					// Update new CBJ list of DTOs
					if (varCount < domainSize && ! RequestChecker.hasEquivDTO(
							pSId, schedDTODomainMap.get(pSId).get(varCount).get(0).getARId())) {
						
						dtoInd = selARIndMap.get(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId());
	
						newSchedARDTOList.clear();
						newSchedARDTOList.add(schedDTODomainMap.get(pSId).get(varCount).get(dtoInd));
					
					} else {
						
						varCount = domainSize - 1;

						newSchedARDTOList = (ArrayList<SchedDTO>) aRDTOList.clone();
					}
				}
				
				// Update scheduled AR indices
//				updateSchedARIndex(pSId, Collections.max(varCountList));
				
				// Clean the list of variable counters
				varCountList.remove(varCountList.size() - 1);
				
//				if (isExitMap.get(pSId)) {
//					
//					break;
//				}
				
			}

			// Update scheduled AR index
			updateSchedARIndex(pSId, varCount);		
			
			logger.info("Rank-based scheduling time for AR: " + aRDTOList.get(0).getARId() 
					+ " equal to (ms): " + (new Date().getTime() - initDate.getTime()));
			
			/**
			 * The planned boolean
			 */
			boolean planned = false;

			RulesPerformer rulesPerformer = new RulesPerformer();
			
			// Compute scheduling solution
			schedSol = (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone();
			
			if (schedSol.size() >= domainSize) {

				logger.info("New plan schedule is updated.");

				planned = true;
			
			} else {
				
				logger.info("New plan schedule is NOT updated.");	
				
				if ((new Date().getTime() - initDate.getTime()) > cutOffTime) {

					logger.warn("Cut-off time for AR: " + aRDTOList.get(0).getARId() + " is reached.");
										
					for (SchedDTO schedDTO : aRDTOList) {
					
						if (! RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTO.getDTOId())) {
						
							/**
							 * The list of rejection reasons
							 */
							ArrayList<ReasonOfRejectElement> rejReasonList = new ArrayList<ReasonOfRejectElement>();
							rejReasonList.add(new ReasonOfRejectElement(ReasonOfReject.systemConflict, 1, "", null));
	
							// Add rejected Id into the map
							RulesPerformer.rejDTORuleListMap.get(pSId).put(schedDTO.getDTOId(), 
									rejReasonList);	
						}
					}
				}
			}

			iterCountMap.clear();

			// Check linked DTOs plan
			if (!planLinkedDTOs(pSId, domainSize, aRDTOList)) {
				
				if (changeLinkedDTOs(pSId, domainSize, aRDTOList)) {
					
					logger.info("A DTO  of the linked DTO is scheduled.");
				
				} else {
					
					logger.info("Linked DTOs are rejected.");
					
					// TODO: to be handled removal of previously scheduled linked DTOs
				}
			}
			
			// Update plan
			updatePlan(pSId, domainSize, planned);

			for (ReasonOfReject reason : ReasonOfReject.values()) {

				conflReasonDTOIdListMap.get(reason).clear();
			}

			conflDTOIdListMap.get(aRDTOList.get(0).getDTOId()).clear();

		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		// Return scheduled solution
		return schedSol;
	}
	
	/**
	 * Initialize CBJ data
	 * 
	 * @param pSId
	 * @param initSchedDomain
	 * @param initSchedSol
	 * @param initSchedARIdList
	 * @param aRDTOList
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	private void initCBJData(Long pSId, ArrayList<ArrayList<SchedDTO>> initSchedDomain, 
			ArrayList<SchedDTO> initSchedSol, ArrayList<String> initSchedARIdList,
			ArrayList<SchedDTO> aRDTOList) throws Exception {
		
//		/**
//		 * Instance handlers
//		 */
//		RulesPerformer rulesPerformer = new RulesPerformer();
		
		logger.debug("Initialize CBJ data.");
		
		// Upgrade iteration Map
		iterMap.put(pSId, iterMap.get(pSId) + 1);

		// Instance minimum AR download time
		minARDwlTimeMap.put(pSId, SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime().getTime());

		// Copy the input domain
		newSchedARDTOList = (ArrayList<SchedDTO>) aRDTOList.clone();

		// Initialize solutions

		logger.debug("Instance initial CBJ scheduling solution.");
		
//		initSchedSol = rulesPerformer.getAcceptedDTOs(pSId);
		
		schedSol = (ArrayList<SchedDTO>) initSchedSol.clone();

		schedARIdList = (ArrayList<String>) initSchedARIdList.clone();
		
		
		// Instance previous scheduling solutions
		if (iterMap.get(pSId) == 1) {

			schedDTODomainMap.get(pSId).addAll((ArrayList<ArrayList<SchedDTO>>) initSchedDomain.clone());

			for (ArrayList<SchedDTO> initSchedDTOList : schedDTODomainMap.get(pSId)) {

				// Initialize maps
				for (SchedDTO schedDTO : initSchedDTOList) {

					conflDTOIdListMap.put(schedDTO.getDTOId(), new ArrayList<String>());

					conflElementListMap.put(schedDTO.getDTOId(), new ArrayList<ElementsInvolvedOnOrbit>());
				}
			}
		}

		// Initialize AR indices map
		for (SchedDTO schedDTO : initSchedSol) {

			initSelARIndMap.put(schedDTO.getARId(), getDTODomainInd(pSId, schedDTO.getDTOId()));
			
			selARIndMap.put(schedDTO.getARId(), getDTODomainInd(pSId, schedDTO.getDTOId()));
		}

		// Initialize actual AR conflict set
		for (SchedDTO schedDTO : aRDTOList) {

			conflDTOIdListMap.put(schedDTO.getDTOId(), new ArrayList<String>());

			conflElementListMap.put(schedDTO.getDTOId(), new ArrayList<ElementsInvolvedOnOrbit>());
		}

		for (ReasonOfReject reason : ReasonOfReject.values()) {

			conflReasonDTOIdListMap.put(reason, new ArrayList<String>());
		}

		schedDTODomainMap.get(pSId).add((ArrayList<SchedDTO>) newSchedARDTOList.clone());

		selARIndMap.put(aRDTOList.get(0).getARId(), 0);
		
	}

	/**
	 * Get the index of the given scheduling AR Id in its domain
	 *
	 * @param pSId
	 *            - the current Planning Session Id
	 * @param domainSize - the size fo the scheduling domain
	 * @param schedARId
	 * 			  -  the scheduling AR Id
	 * @return
	 */
	private int getARDomainInd(Long pSId, int domainSize, String schedARId) {

		/**
		 * The output index
		 */
		int index = 0;

		for (int i = 0; i < domainSize; i++) {

			if (schedDTODomainMap.get(pSId).get(i).get(0).getARId().equals(schedARId)) {

				// Set index
				index = i;
			}
		}

		return index;
	}

	/**
	 * Get the index of the given AR Id in the scheduling domain
	 *
	 * @param pSId
	 *            - the current Planning Session Id
	 * @param conflschedARId
	 * @param dtoInd
	 * @return
	 */
	private int getDTODomainInd(Long pSId, String schedDTOId) {
		
		/**
		 * The output index
		 */
		int index = 0;

		if (schedDTOId != null) {
		
			for (ArrayList<SchedDTO> schedDTOList : schedDTODomainMap.get(pSId)) {
	
				if (RequestChecker.isStandardAR(pSId, ObjectMapper.getSchedARId(schedDTOId))) { // ordered index
				
					for (int i = 0; i < schedDTOList.size(); i++) {
		
						if (schedDTOList.get(i).getDTOId().equals(schedDTOId)) {
		
							// Set index
							index = i;
							
							break;
						}
					}
				
				} else { // maximum index
					
					index = schedDTOList.size() - 1;
				}
			}
			
		} else {
		
//			logger.debug("No solution found yet for the AR.");
		}

		return index;
	}
	
	/**
	 * Get the scheduled DTO id  of the given AR Id
	 *
	 * @param schedARId
	 * @return the relevant scheduled DTOId
	 */
	private String getSchedSolDTOId(String schedARId) {
		
		/**
		 * The output index
		 */
		String schedDTOId = null;

		for (SchedDTO schedDTO : schedSol) {

			if (schedDTO.getARId().equals(schedARId)) {
				
				schedDTOId = schedDTO.getDTOId();
			}				
		}

		return schedDTOId;
	}


	/**
	 * Update plan data according to BRM scheduling
	 * solution
	 *
	 * @param pSId
	 *            - the current Planning Session Id
	 * @param planned - the planned boolean
	 * @param domainSize - the size of the scheduling domain
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void updatePlan(Long pSId, int domainSize, boolean planned) throws Exception {
		
		/**
		 * Instance handler
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();
			
		logger.info("Update the scheduling plan for Planning Session: " + pSId);
		
		if (planned) {

//			// Manual Session type
//			if (SessionChecker.isManualSession(pSId)) {
//
//				logger.info("Manual AR " + ManualPlanProcessor.manPlanARMap.get(pSId).getARId() 
//						+ " has been scheduled.");
//
//			// Ranked Session type
//			} else {
//
//				logger.info("Next AR " + NextARProcessor.nextSchedARMap.get(pSId).getARId() 
//						+ " has been scheduled.");
//			}

		} else {

			// Manual Session type
			if (SessionChecker.isManual(pSId)) {

				logger.info("Manual AR " + ManualPlanProcessor.manPlanARMap.get(pSId).getARId()
						+ " has NOT been scheduled.");
			
			// Ranked Session type
			} else {

				logger.info("Next AR " + NextARProcessor.nextSchedARMap.get(pSId).getARId() 
						+ " has NOT been scheduled.");
			}

			// Reset scheduled solution with BRM status
			logger.info("Reset scheduled solution statuses accordingly.");

			if ((initRejDTOIdListMap != null)) {

				/**
				 * The reason of reject Id list iterator
				 */
				Iterator<Entry<String, List<ReasonOfRejectElement>>> it = initRejDTOIdListMap.entrySet().iterator();

				while (it.hasNext()) {

					/**
					 * The reason of reject iterator
					 */
					Map.Entry<String, List<ReasonOfRejectElement>> entry = it.next();

					if ((entry.getValue().size() == 1)
							&& entry.getValue().get(0).getReason().equals(ReasonOfReject.deletedByCsps)) {

						it.remove();
					}
				}
				
			} else {

				logger.info("Empty list of rejected DTO Ids.");
			}

			logger.debug(("Reset solution to initial status."));
			
			if (initSchedSol != null) {
				
				schedSol = (ArrayList<SchedDTO>) initSchedSol.clone();
			}
			
			//Replan missing DTOs
			for (SchedDTO schedDTO : schedSol) {
							
				if (!schedARIdList.contains(schedDTO.getARId())) {				
					schedARIdList.add(schedDTO.getARId());
				}
				
				if (!RulesPerformer.getPlannedARIds(pSId).contains(schedDTO.getARId())) {
					
					if (RequestChecker.hasEquivDTO(pSId, schedDTO.getARId())) {
						
						logger.warn("An equivalent DTO is rejected and replanned for AR: " 
						+ schedDTO.getARId());
						
						/**
						 * The Equivalent DTO
						 */
						EquivalentDTO equivDTO = ObjectMapper.parsePrevBRMEquivDTO(pSId,
								PRListProcessor.aRSchedIdMap.get(pSId).get(schedDTO.getARId()).getEquivalentDTO());
						
						rulesPerformer.planEquivDTO(pSId, equivDTO, false);
									
					} else {
						
						rulesPerformer.planSchedDTO(pSId, schedDTO);
						
						selARIndMap.put(schedDTO.getARId(), getDTODomainInd(pSId, schedDTO.getDTOId()));

					}
				}
			}
			
			// Update rejected DTOs
			rulesPerformer.setRejectedDTOs(pSId);

			// Reset scheduling domain and solution
			if (!schedDTODomainMap.get(pSId).isEmpty()) {

				schedDTODomainMap.get(pSId).remove(domainSize - 1);
			}
		}
	}

	/**
	 * Update scheduled AR index according to the present solution
	 *
	 * @param pSId - the current Planning Session Id
	 * @throws Exception
	 */
	private void updateSchedARIndex(Long pSId, int var) throws Exception {

		logger.debug("Update scheduling AR selection indices.");

		// Update selected AR indices
		for (int i = 0; i <= var; i ++) {
			
			String schedARId = schedDTODomainMap.get(pSId).get(i).get(0).getARId(); 
		
			selARIndMap.put(schedARId, 
					getDTODomainInd(pSId, getSchedSolDTOId(schedARId)));
		}
		
		logger.debug("Size of scheduled solution equals to " + schedSol.size() 
			+ " for Planning Session: " + pSId);
	}

	/**
	 * Iterate Conflict back-Jumping 
	 * // TODO: check and test for multiple rules
	 * // TODO: arrange first element of the conflict list for varCount
	 *
	 * @param pSId
	 *            - the Planning Session Id
	 * @param varCountList
	 *            - the list of variable counters
	 * @param varCount
	 *            - the variable counter
	 * @param dtoInd
	 *            - the DTO index
	 * @param cutOffTime
	 *            - the cutoff time
	 * @return the new variable counter
	 * @throws Exception
	 */
	private int iterCBJ(Long pSId, ArrayList<Integer> varCountList, int varCount, 
			HashMap<Integer, Integer> iterCountMap, int conflCountIter, int dtoInd, 
			double cutOffTime) throws Exception {

		/**
		 * The previous variable counter
		 */
		int preVarCount = varCount;

		/**
		 * The iteration date
		 */
		Date iterDate = new Date();

		/**
		 * The exit boolean
		 */
		boolean exit = false;

		/**
		 * Index of empty conflict list
		 */
		int ind = 0;

		/**
		 * The scheduling domain size
		 */
		int domainSize = schedDTODomainMap.get(pSId).size();
		
		while (ind < schedDTODomainMap.get(pSId).get(varCount).size() && 
				conflDTOIdListMap.get(schedDTODomainMap.get(pSId)
				.get(varCount).get(dtoInd).getDTOId()).isEmpty()) {			
			
			// update DTO index
			dtoInd ++;

			if (dtoInd >= schedDTODomainMap.get(pSId).get(varCount).size()) {

				dtoInd = dtoInd - schedDTODomainMap.get(pSId).get(varCount).size();
			}
			
			ind ++;
		}
		
		selARIndMap.put(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId(), dtoInd);
		initSelARIndMap.put(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId(), dtoInd);
				
		while (!conflDTOIdListMap.get(schedDTODomainMap.get(pSId)
				.get(varCount).get(dtoInd).getDTOId()).isEmpty()
				&& ((new Date().getTime() - iterDate.getTime()) < cutOffTime)
				&& ! checkCBJExitCondition(pSId, domainSize, varCount, varCountList)) {
			
			preVarCount = varCount;

			/**
			 * The previous DTO index
			 */
			int preDTOInd = dtoInd;

			// Get the AR domain index
			varCount = getARDomainInd(pSId, domainSize, ObjectMapper.getSchedARId(
					conflDTOIdListMap.get(schedDTODomainMap.get(pSId)
							.get(preVarCount).get(dtoInd).getDTOId()).get(conflCountIter))); 
			
			// Iter element of the conflict list
			varCountList.add(varCount);
			
			// Update DTO index
			dtoInd = selARIndMap.get(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId()) + 1;

			if (dtoInd >= schedDTODomainMap.get(pSId).get(varCount).size()) {

				dtoInd = dtoInd - schedDTODomainMap.get(pSId).get(varCount).size();
			}

			selARIndMap.put(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId(), dtoInd);

			// Find consistent AR DTO List
			int iterCount = 0;

			for (int i = 0; i < schedDTODomainMap.get(pSId).get(varCount).size(); i++) {

				iterCount = dtoInd + i;
				
				if (iterCountMap.containsKey(varCount)) {
				
					iterCountMap.put(varCount, iterCountMap.get(varCount) + 1);
				
				} else {
					
					iterCountMap.put(varCount, 0);
				}

				if (iterCount >= schedDTODomainMap.get(pSId).get(varCount).size()) {

					iterCount -= (schedDTODomainMap.get(pSId).get(varCount).size());
				}				
				
				logger.debug("Var Count: " + varCount + "; Iter Count: " + iterCount);
				
				newSchedARDTOList.clear();
				newSchedARDTOList.add(schedDTODomainMap.get(pSId).get(varCount).get(iterCount));

				// Check the DTO consistency
				if (!isOverlapDTO(pSId, varCount, iterCount, dtoInd)
					&& getIterDTO(pSId, false, domainSize, varCount, varCountList) != null) {

					jumpMap.put(pSId, jumpMap.get(pSId) + 1);
					
					dtoInd = selARIndMap.get(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId());
					
					updateSchedARIndex(pSId, varCount);
					
					break;

				} else {

//					// Restore the previously scheduled DTOs
//					restorePrevSchedDTOs(pSId, Collections.max(varCountList), 
//							(ArrayList<Integer>) varCountList.clone());
				}

				if (i == (schedDTODomainMap.get(pSId).get(varCount).size() - 1)) {

					varCount = preVarCount;
					dtoInd = preDTOInd;
				
				} else {
					
//					// TODO: Added from CSPS 0.14.1 -------
					// TODO: Removed from CSPS 1.0.0 -------
//					
//					if (iterCountMap.get(varCount) >= schedDTODomainMap.get(pSId).get(varCount).size()) {
//										
//						conflCountIter ++;
//						
//						break;
//					}
//				
//					// ---------------
				}

				
//				// Restore the previously scheduled DTOs
//				if (restorePrevSchedDTOs(pSId,  Collections.max(varCountList), 
//						(ArrayList<Integer>) varCountList.clone())) {
//
//					selARIndMap.put(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId(), dtoInd);
//
//				} else {
//
//					exit = true;
//
//					break;
//				}
			}

			// Exit if not restored the previously scheduled DTOs
			if (exit) {

				break;
			}
			
		}

		return conflCountIter;
	}

	 /** 
	  * Check if exists overlap between DTOs
	  * 
	 * @param pSId
	 * @param varCount
	 * @param iterCount
	 * @param dtoInd
	 * @return
	 */
	private static boolean isOverlapDTO(Long pSId, int varCount, int iterCount, int dtoInd) {
		
		/**
		 * The overlap boolean
		 */
		boolean isOverlap = false;
		
		// Check overlapping condition 
		if (iterCount != dtoInd) {
		
			if ((schedDTODomainMap.get(pSId).get(varCount).get(iterCount).getStartTime().getTime() <=
					schedDTODomainMap.get(pSId).get(varCount).get(dtoInd).getStartTime().getTime() &&
					schedDTODomainMap.get(pSId).get(varCount).get(iterCount).getStopTime().getTime() >=
					schedDTODomainMap.get(pSId).get(varCount).get(dtoInd).getStartTime().getTime())
					|| (schedDTODomainMap.get(pSId).get(varCount).get(iterCount).getStartTime().getTime() <=
							schedDTODomainMap.get(pSId).get(varCount).get(dtoInd).getStopTime().getTime() &&
							schedDTODomainMap.get(pSId).get(varCount).get(iterCount).getStopTime().getTime() >=
							schedDTODomainMap.get(pSId).get(varCount).get(dtoInd).getStopTime().getTime())) {
				
				// Set overlap				
				isOverlap = true;
			}
		}
	
		return isOverlap;
	}
	
	/**
	 * Check if the DTOs of a given AR are arc-consistent with respect to previous
	 * scheduled DTOs
	 * 
	 * @param pSId - the Planning Session Id
	 * @param domainSize - the size of the scheduling domain
	 */
	private void checkArcConsistency(Long pSId, int domainSize) {

		logger.debug("Check arc-consistency of the new AR DTOs.");
		
		// Check arc consistency
		for (SchedDTO schedDTO : schedDTODomainMap.get(pSId).get(domainSize - 1)) {

			for (int i = 0; i < (domainSize - 1); i++) {
				
				// Check DTO Domain
				if (schedDTODomainMap.get(pSId).get(i).size() == 1) {

					if (IntMatrixCalculator.intDTOMatrixMap.get(pSId).containsKey(schedDTO.getDTOId())
							&& IntMatrixCalculator.intDTOMatrixMap.get(pSId).get(schedDTO.getDTOId())
								.containsKey(schedDTODomainMap.get(pSId).get(i).get(0).getDTOId())) {

						if (IntMatrixCalculator.intDTOMatrixMap.get(pSId).get(schedDTO.getDTOId())
								.get(schedDTODomainMap.get(pSId).get(i).get(0).getDTOId()) == 0) {

							// Remove scheduling DTO
							schedDTODomainMap.get(pSId).get(domainSize - 1).remove(schedDTO);
						}
					}
				}
			}
		}
	}
	
	/**
	 * Check the CBJ exit condition
	 * Changed on 29/07/2021
	 *
	 * @param pSId
	 *            - the current planning session Id
	 * @param domainSize - the size of the scheduling domain
	 * @param varCount
	 *            - the variable counter
	 * @param varCountList - the list of variable counters
	 * @return true if to exit
	 * @throws Exception 
	 */
	private synchronized boolean checkCBJExitCondition(Long pSId, int domainSize, int varCount, 
			ArrayList<Integer> varCountList)  throws Exception {

		/**
		 * The exit boolean
		 */
		isExitMap.put(pSId, false);
		
		/**
		 * Exit counter
		 */
		int exitCount = 0;

		/**
		 * Conflict counter 
		 */
		int conflCount = 0;

		logger.info("Check CBJ Exit condition...");
		
//		// Restore previously scheduled DTOs
//		restorePrevSchedDTOs(pSId, varCount, (ArrayList<Integer>) varCountList.clone());
		
		// -----------------------------
				
		for (SchedDTO schedDTO : schedDTODomainMap.get(pSId).get(varCount)) {

			for (String conflDTOId : conflDTOIdListMap.get(schedDTO.getDTOId())) {
				
				// Added on 7/7/2020 and 31/8/2020
				if (RequestChecker.isStandardAR(pSId, ObjectMapper.getSchedARId(conflDTOId))
						&& hasConflictElements(pSId, schedDTO.getDTOId())) {

					conflCount ++;
					
					/**
					 * The AR DTO index of the selected AR at iteration
					 */
					int iterARDTOInd = 0;
							
					if (selARIndMap.containsKey(ObjectMapper.getSchedARId(conflDTOId))) {
					
						iterARDTOInd = selARIndMap.get(ObjectMapper.getSchedARId(conflDTOId));
					
					} else {
						
						logger.warn("No selection index found for AR: " + ObjectMapper.getSchedARId(conflDTOId));
					}
					
					/**
					 * The maximum DTO index of the selected AR
					 */
					int maxSelARDTOInd =  schedDTODomainMap.get(pSId).get(
							getARDomainInd(pSId, domainSize, ObjectMapper.getSchedARId(conflDTOId))).size() - 1;
					
					if (iterARDTOInd == maxSelARDTOInd) {
	
						exitCount ++;
	
						break;
					}
				}
			}
		}

		logger.debug("Exit Count: " + exitCount + "; Confl Count: " + conflCount);
		
		if (exitCount >= conflCount) {

			logger.info("CBJ Exit condition is reached for the AR: " 
					+ schedDTODomainMap.get(pSId).get(varCount).get(0).getARId());

			isExitMap.put(pSId, true);
		}

		return isExitMap.get(pSId);
	}

	/**
	 * Select a valid DTO according to the inference process within the BRM
	 * scheduling rules
	 *
	 * @param pSId
	 *            - the current planning session Id
	 * @param firstGuess
	 *            - the first guess boolean
	 * @param varCount
	 *            - the variable counter
	 * @return the selected DTO, if consistent
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private SchedDTO selectValidDTO(Long pSId, boolean firstGuess, int varCount) throws Exception {

		/**
		 * The output boolean
		 */
		SchedDTO selSchedDTO = null;
		
		/**
		 * Instance handler
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		// Retrieve initial status of the session
		if (firstGuess) {

			initSchedSol = (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone();

			initRejDTOIdListMap = (HashMap<String, List<ReasonOfRejectElement>>) RulesPerformer.rejDTORuleListMap
					.get(pSId).clone();
		}
		
		// Retract conflicting DTOs
		rulesPerformer.retractDTOList(pSId, (ArrayList<SchedDTO>) schedDTODomainMap.get(pSId).get(varCount).clone(), 
				ReasonOfReject.deletedByCsps);

		// Cycle for new DTOs
		for (int i = 0; i < newSchedARDTOList.size(); i++) {

			logger.debug("DTO Count: " + i);
			
			/**
			 * The selected DTO
			 */
			selSchedDTO = newSchedARDTOList.get(i);

			// Select best element of DTO domain
			logger.info("Selected DTO at iteration: " + selSchedDTO.getDTOId());

			//  Check DTO according to the minimum AR download time
			if (selSchedDTO.getStartTime().getTime() < minARDwlTimeMap.get(pSId)) {

				minARDwlTimeMap.put(pSId, selSchedDTO.getStartTime().getTime());
			}

			// consistency boolean
			boolean consistency = true;

			// Clear initial map
			conflDTOIdListMap.get(selSchedDTO.getDTOId()).clear();

			// Check consistency of the scheduling rules within BRM
			if (rulesPerformer.planSchedDTO(pSId, selSchedDTO)) { 

				// Compute scheduling solution
				schedSol = (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone();
				
				schedARIdList = new ArrayList<>();
				
				for (SchedDTO schedDTO : schedSol) {			
					
					if (!schedARIdList.contains(schedDTO.getARId())) {
						schedARIdList.add(schedDTO.getARId());
					}
				}
				
			} else {
				
				// Add conflicting DTOs
				if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(selSchedDTO.getDTOId())) {

					// Set conflict elements if existing
					setRankedConflictElements(pSId, selSchedDTO.getDTOId());
				}

				// Update consistency
				consistency = false;
				
				selSchedDTO = null;
			}

			if (consistency) {

				logger.info("Selected DTO: " + selSchedDTO.getDTOId() + " is consistent.");
				
				// Return selected DTO
				break;
			}
			
			jumpMap.put(pSId, jumpMap.get(pSId) + 1);
		}

		if (selSchedDTO == null) {
		
			logger.debug("DTOs of the selected AR: " + newSchedARDTOList.get(0).getARId() 
					+ " are inconsistent.");		
		}

		// Return output
		return selSchedDTO;
	}
	
	/**
	 * Select a valid DTO according to the inference process within the BRM
	 * scheduling rules
	 *
	 * @param pSId
	 *            - the current planning session Id
	 * @param firstGuess
	 *            - the first guess boolean
	 * @param domainSize 
	 * 			  - the size of the scheduling domain
	 * @param varCount
	 *            - the variable counter
	 * @return the selected DTO, if consistent
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private SchedDTO selectValidPairDTO(Long pSId, boolean firstGuess, int domainSize, 
			int varCount, ArrayList<Integer> varCountList) throws Exception {

		/**
		 * The output boolean
		 */
		SchedDTO selSchedDTO = null;
		
		/**
		 * Instance handler
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		// Retrieve initial status of the session
		if (firstGuess) {

			initSchedSol = (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone();

			initRejDTOIdListMap = (HashMap<String, List<ReasonOfRejectElement>>) RulesPerformer.rejDTORuleListMap
					.get(pSId).clone();
		}
		
		// Retract conflicting DTOs
		rulesPerformer.retractDTOList(pSId, (ArrayList<SchedDTO>) 
				schedDTODomainMap.get(pSId).get(varCount).clone(), 
				ReasonOfReject.deletedByCsps);
		
		for (int l = 0; l < schedDTODomainMap.get(pSId).get(varCount).size(); l++) {
		
			// consistency boolean
			boolean consistency = true;
			
			/**
			 * The pair DTO list
			 */
			ArrayList<SchedDTO> pairDTOList = new ArrayList<SchedDTO>();
			
			selSchedDTO = schedDTODomainMap.get(pSId).get(varCount).get(l);
			
			pairDTOList.add(selSchedDTO);
			
			logger.debug("Int Count: " + l);

			// Select best element of DTO domain
			logger.info("Selected DTO at iteration: " + selSchedDTO.getDTOId());

			//  Check DTO according to the minimum AR download time
			if (selSchedDTO.getStartTime().getTime() < minARDwlTimeMap.get(pSId)) {

				minARDwlTimeMap.put(pSId, selSchedDTO.getStartTime().getTime());
			}

			/**
			 * The linked DTO
			 */
			SchedDTO linkDTO = PRListProcessor.schedDTOMap.get(pSId).get(
					schedDTODomainMap.get(pSId).get(varCount).get(l).getLinkDtoIdList().get(0));

			
			// Clear selected DTO conflict map
			if (conflDTOIdListMap.containsKey(selSchedDTO.getDTOId())) {
			
				conflDTOIdListMap.get(selSchedDTO.getDTOId()).clear();
			}
			
			if (linkDTO != null) {
				
				// Add linked DTO
				pairDTOList.add(linkDTO); // TBD: null case
				
				// Clear linked DTO conflict map
				if (conflDTOIdListMap.containsKey(linkDTO.getDTOId())) {

					conflDTOIdListMap.get(linkDTO.getDTOId()).clear();
				}
			}

			// Check consistency of the scheduling rules within BRM
			if (rulesPerformer.planSchedDTOList(pSId, pairDTOList, true)) { 

				// Compute scheduling solution
				schedSol = (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone();
				
				schedARIdList = new ArrayList<>();
				
				for (SchedDTO schedDTO : schedSol) {			
					
					if (!schedARIdList.contains(schedDTO.getARId())) {					
						schedARIdList.add(schedDTO.getARId());
					}
				}
				
			} else {
				
				for (SchedDTO schedDTO : pairDTOList) {
				
					// Add conflicting DTO
					if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTO.getDTOId())) {
	
						// Set conflict elements if existing
						setRankedConflictElements(pSId, schedDTO.getDTOId());
					
					} else if (checkCBJExitCondition(pSId, domainSize, varCount, varCountList)) {
						
						// Retract stereo pair DTO
						rulesPerformer.retractDTOById(pSId, schedDTO.getDTOId(), ReasonOfReject.cannotPerformStereopair);
					}
				}

				// Update consistency
				consistency = false;
				
				selSchedDTO = null;
			}

			if (consistency) {

				logger.info("Selected DTO: " + selSchedDTO.getDTOId() + " is consistent.");
				
				// Return selected DTO
				break;
			}
			
			jumpMap.put(pSId, jumpMap.get(pSId) + 1);
		}

		if (selSchedDTO == null) {
		
			logger.debug("DTOs of the selected AR: " + schedDTODomainMap.get(pSId).get(varCount).get(0).getARId() 
					+ " are inconsistent.");		
		}

		// Return output
		return selSchedDTO;
	}

	/**
	 * Select a valid Equivalent DTO according to the inference process within the
	 * BRM scheduling rules
	 *
	 * @param pSId
	 *            - the current planning session Id
	 * @param firstGuess
	 *            - the first guess boolean
	 * @param varCount
	 *            - the variable counter
	 * @return the first DTO of the equivalent one, if consistent
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private SchedDTO selectValidEquivDTO(Long pSId, boolean firstGuess, int varCount) throws Exception {

		/**
		 * The output boolean
		 */
		SchedDTO selSchedDTO = null;
		
		/**
		 * Instance handler
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		// Retrieve initial status of the session
		if (firstGuess) {

			initSchedSol = (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone();

			initRejDTOIdListMap = (HashMap<String, List<ReasonOfRejectElement>>) RulesPerformer.rejDTORuleListMap
					.get(pSId).clone();
		}

		// Retract conflicting DTOs
		rulesPerformer.retractDTOList(pSId, (ArrayList<SchedDTO>) schedDTODomainMap.get(pSId).get(varCount).clone(), ReasonOfReject.deletedByCsps);

		logger.info("Selected Equivalent DTO at iteration for AR: "
				+ schedDTODomainMap.get(pSId).get(varCount).get(0).getARId());

		/**
		 * The list of scheduled DTOs
		 */
		ArrayList<SchedDTO> schedDTOList = new ArrayList<>();

		// While new DTOs exist
		for (int i = 0; i < newSchedARDTOList.size(); i++) {

			/**
			 * The selected DTO
			 */
			selSchedDTO = newSchedARDTOList.get(i);

			// Clear initial map
			conflDTOIdListMap.get(selSchedDTO.getDTOId()).clear();

			schedDTOList.add(selSchedDTO);
		}

		/**
		 * The consistency boolean
		 */
		boolean consistency = true;

		/**
		 * The equivalent DTO
		 */
		EquivalentDTO equivDTO = new EquivalentDTO();

		// Manual Session type
		if (SessionChecker.isManual(pSId)) {

			equivDTO = ObjectMapper.parseSchedToBRMEquivDTO(pSId, ManualPlanProcessor.manPlanARMap.get(pSId),
					ManualPlanProcessor.manPlanARMap.get(pSId).getEquivalentDTO());

			equivDTO.setAllDtoInEquivalentDto(ObjectMapper.parseSchedToBRMDTOList(pSId, schedDTOList));

		// Ranked Session type  
		} else {

			equivDTO = ObjectMapper.parseSchedToBRMEquivDTO(pSId, NextARProcessor.nextSchedARMap.get(pSId),
					NextARProcessor.nextSchedARMap.get(pSId).getEquivalentDTO());

			equivDTO.setAllDtoInEquivalentDto(ObjectMapper.parseSchedToBRMDTOList(pSId, schedDTOList));

		}

		if (!rulesPerformer.planEquivDTO(pSId, equivDTO, false)) {

			for (SchedDTO schedDTO : schedDTOList) {

				// Add conflicting DTOs
				if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTO.getDTOId())) {

					// Set conflict elements if existing
					setRankedConflictElements(pSId, schedDTO.getDTOId());
				}
			}

			// Update consistency
			consistency = false;
			
			selSchedDTO = null;
		}

		// Update scheduling solution
		schedSol = (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone();

		schedARIdList = new ArrayList<>();
		
		for (SchedDTO schedDTO : schedSol) {			
			
			if (!schedARIdList.contains(schedDTO.getARId())) {			
				schedARIdList.add(schedDTO.getARId());
			}
		}
		
		if (consistency) {

			logger.info("Selected AR Equivalent DTO: " + equivDTO.getEquivalentDtoId() + " is consistent.");

			// // TODO: define
			// selARIndMap.put(schedDTODomainMap.get(pSId).get(varCount).get(0).getARId(),
			// getDTOInd(pSId, varCount, selSchedDTO.getDTOId()));

			// Return selected DTO
			selSchedDTO = newSchedARDTOList.get(0);
			

			if (equivDTO.getDi2sInfo() != null) {
			
				logger.info("The DI2S DTO is consistent within the BRM.");

				EquivDTOHandler.di2sLinkedIdsMap.get(pSId).put(equivDTO.getDi2sInfo().getRelativeMasterId(), 
						equivDTO.getDi2sInfo().getRelativeSlaveId());
			}

		} else {
			
			logger.debug("Equivalent DTO of selected AR are inconsistent.");
		}

		// Return inconsistency
		return selSchedDTO;

	}

	/**
	 * Set the conflict elements for the given DTO Id
	 *
	 * // TODO: test if conflict element correspond to an Equivalent DTO it should
	 * be not an applicable CBJ node 
	 * // TODO: check that first conflicting index
	 * list corresponds to the most valuable
	 *
	 * @param pSId
	 * @param schedDTOId
	 */
	public void setRankedConflictElements(Long pSId, String schedDTOId) {

		try {
						
			if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTOId)) {
	
				for (ReasonOfRejectElement reason : RulesPerformer.rejDTORuleListMap.get(pSId).get(schedDTOId)) {
	
					/**
					 * The entrySet iterator
					 */
					Iterator<Entry<Double, List<ElementsInvolvedOnOrbit>>> it = reason.getElementsInvolved().entrySet()
							.iterator();
	
					while (it.hasNext()) {
	
						for (ElementsInvolvedOnOrbit element : it.next().getValue()) {
	
							if (((element.getElementsInvolved() != null) 
									&& (conflReasonDTOIdListMap.get(reason.getReason()) != null 
									&& !conflReasonDTOIdListMap.get(reason.getReason()).containsAll(element.getElementsInvolved())))
									|| (ConflictDTOCalculator.isConflictReason(reason.getReason()))) {
	
								// Add conflict element
								if (conflElementListMap.containsKey(schedDTOId)) {
								
									conflElementListMap.get(schedDTOId).add(element);
								}
	
								if (!conflReasonDTOIdListMap.get(reason.getReason()).isEmpty()
									&& !conflReasonDTOIdListMap.get(reason.getReason())
												.containsAll(element.getElementsInvolved())) {
	
									// Add involved elements
									conflReasonDTOIdListMap.get(reason.getReason()).addAll(
											element.getElementsInvolved());
								}
							}
						}
					}
				}
	
				// Filter conflict element list
				filterConflElemList(pSId, schedDTOId);
				
				// Sort the conflicting DTO Ids based on their cost
				if (!conflElementListMap.get(schedDTOId).isEmpty()) {
					
					sortConflDTOList(pSId, schedDTOId);
				}
			}
			
		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}
	
	
	/**
	 * Check if the DTO has conflict elements
	 *
	 * @param pSId
	 * @param schedDTOId
	 */
	public boolean hasConflictElements(Long pSId, String schedDTOId) {

		/**
		 * The conflict boolean
		 */
		boolean isConflict = false;
		
		try {
						
			if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTOId)) {
	
				for (ReasonOfRejectElement reason : RulesPerformer.rejDTORuleListMap.get(pSId).get(schedDTOId)) {
	
					if (ConflictDTOCalculator.isCBJConflictReason(reason.getReason())) {
						
						isConflict = true;
						
						break;
					}
				}
			}
				
		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
			
		return isConflict;		
	}


	/**
	 * Filter list of conflict elements
	 * @param pSId
	 * @param schedDTOId
	 * @throws Exception 
	 */
	private void filterConflElemList(Long pSId, String schedDTOId) throws Exception {
		
		logger.debug("Filter conflicting elements for DTO: " + schedDTOId);

		
		// Filter equivalent DTOs from the list of conflict elements
		for (int i = 0; i < conflElementListMap.get(schedDTOId).size(); i++) {

			/** 
			 * The involved elements
			 */
			ElementsInvolvedOnOrbit element = conflElementListMap.get(schedDTOId).get(i);

			if (element.getElementsInvolved() != null) {
				
				for (int j = 0; j < element.getElementsInvolved().size(); j++) {

					/** 
					 * The conflicting DTO Id
					 */
					String conflDTOId = element.getElementsInvolved().get(j);

					if (RequestChecker.hasEquivDTO(pSId, ObjectMapper.getSchedARId(conflDTOId))
							|| ! RequestChecker.isStandardAR(pSId, ObjectMapper.getSchedARId(conflDTOId))
							|| ! PRListProcessor.schedDTOMap.get(pSId).containsKey(conflDTOId)
							|| RequestChecker.isLinkDTOScheduled(pSId, PRListProcessor.schedDTOMap.get(pSId).get(conflDTOId))
							|| PRListProcessor.schedDTOMap.get(pSId).get(conflDTOId).getStartTime().compareTo(
									SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime()) < 0) {

						logger.trace("The DTO: " + conflDTOId + " is removed from the conflict list.");
						element.getElementsInvolved().remove(conflDTOId);
					}	
				}

				// Set element
				conflElementListMap.get(schedDTOId).set(i, element);			
			} 
		}
	}
	
	/**
	 * Sort conflicting DTO by cost
	 * @param pSId
	 * @param schedDTOId
	 * @throws Exception 
	 */
	private void sortConflDTOList(Long pSId, String schedDTOId) throws Exception {
		
		logger.debug("Sort conflicting DTOs with DTO: " + schedDTOId);
		
		// Sort conflicts by cost
		sortConflictsByCost(pSId, schedDTOId);

		// Filter equivalent DTOs from the list of conflict elements
		for (int i = 0; i < conflElementListMap.get(schedDTOId).size(); i++) {

			/** 
			 * The involved elements
		     */
			ElementsInvolvedOnOrbit element = conflElementListMap.get(schedDTOId).get(i);

			if (element.getElementsInvolved() != null) {

				for (String conflDTOId : element.getElementsInvolved()) {

					if (!conflDTOId.equals(schedDTOId) 
							&& !conflDTOIdListMap.get(schedDTOId).contains(conflDTOId)
							&& PRListProcessor.schedDTOMap.get(pSId).containsKey(conflDTOId)
							&& ! RequestChecker.isLinkDTOScheduled(pSId, PRListProcessor.schedDTOMap.get(pSId).get(conflDTOId))
							&& PRListProcessor.schedDTOMap.get(pSId).get(conflDTOId).getStartTime().compareTo(
									SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime()) > 0) {

						conflDTOIdListMap.get(schedDTOId).add(conflDTOId);
					}
				}				 
			}
		}

		if (!conflDTOIdListMap.get(schedDTOId).isEmpty()) {

			logger.debug("A number of candidate conflicting DTOs found: " 
			+ conflDTOIdListMap.get(schedDTOId).size());
			
//			logger.info("Candidate conflicting DTO found: " 
//			+ conflDTOIdListMap.get(selSchedDTOId).get(0));	
		}

		// Reset conflict element list for the selected DTO
		conflElementListMap.get(schedDTOId).clear();
	}
	
	/**
	 * Sort conflicting DTOs by cost (frequency, proximity, size), then add to the
	 * conflict Id list // TODO: test
	 *
	 * @param pSId
	 * @param selSchedDTOId
	 */
	@SuppressWarnings("unchecked")
	private void sortConflictsByCost(Long pSId, String selSchedDTOId) {

		/**
		 * The counter map
		 */
		Map<String, Integer> counterMap = new HashMap<>();

		/**
		 * The list of conflicting DTOs
		 */
		ArrayList<SchedDTO> conflDTOList = new ArrayList<>();

		for (ElementsInvolvedOnOrbit elementList : conflElementListMap.get(selSchedDTOId)) {

			if (elementList.getElementsInvolved() != null) {
				
				for (String schedDTOId : elementList.getElementsInvolved()) {
					
					if (PRListProcessor.schedDTOMap.get(pSId).containsKey(schedDTOId)
							&& PRListProcessor.schedDTOMap.get(pSId).containsKey(selSchedDTOId)
							&& !conflDTOList.contains(PRListProcessor.schedDTOMap.get(pSId).get(schedDTOId))) {
						
						if (PRListProcessor.schedDTOMap.get(pSId).get(schedDTOId).getStartTime().compareTo(
								SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime()) > 0) {
						
							counterMap.put(schedDTOId, 1 + (counterMap.containsKey(schedDTOId) ? counterMap.get(schedDTOId) : 0));

							PRListProcessor.schedDTOMap.get(pSId).get(schedDTOId).setDeltaTime(
										Math.abs(PRListProcessor.schedDTOMap.get(pSId).get(selSchedDTOId).getStartTime().getTime()
												- PRListProcessor.schedDTOMap.get(pSId).get(schedDTOId).getStartTime().getTime()));
			
							conflDTOList.add(PRListProcessor.schedDTOMap.get(pSId).get(schedDTOId));
						}
	
					}
				}	
			}
		}

		logger.debug("Sort conflicting indices by worth.");

		// 1. Sorting by frequency
		@SuppressWarnings("rawtypes")
		ArrayList freqDTOIdList = new ArrayList(counterMap.keySet());

		Collections.sort(freqDTOIdList, new Comparator<String>() {

			/**
			 * Compare counters
			 * 
			 * @param x
			 * @param y
			 */
			@Override
			public int compare(String x, String y) {

				return counterMap.get(y) - counterMap.get(x);
			}
		});

		// 2. Sorting by proximity
		Collections.sort(conflDTOList, new SchedDTODeltaTimeComparator());

		ArrayList<String> proxDTOIdList = new ArrayList<>();
		for (SchedDTO schedDTO : conflDTOList) {

			proxDTOIdList.add(schedDTO.getDTOId());
		}

		// 3. Sorting by size
		Collections.sort(conflDTOList, new SchedDTOSizeComparator());

		ArrayList<String> sizeDTOIdList = new ArrayList<>();

		for (SchedDTO schedDTO : conflDTOList) {

			sizeDTOIdList.add(schedDTO.getDTOId());
		}

		// 4. Global sorting according to frequency, proximity and size
		ArrayList<ArrayList<String>> worthList = new ArrayList<>();
		worthList.add(freqDTOIdList);
		worthList.add(proxDTOIdList);
		worthList.add(sizeDTOIdList);

		/**
		 * The finally sorted conflict Id list
		 */
		conflDTOIdListMap.put(selSchedDTOId, sortWorthDTOs(worthList));
	}

	/**
	 * Worth DTOs by lower priority index 
	 * // TODO: TBC if lists related to add scopes to be extended
	 *
	 * @param worthList
	 * @return
	 */
	private ArrayList<String> sortWorthDTOs(ArrayList<ArrayList<String>> worthList) {

		/**
		 * The output list
		 */
		ArrayList<String> sortedDTOIdList = new ArrayList<>();

		/**
		 * The worth DTO map
		 */
		HashMap<String, Integer> worthDTOMap = new HashMap<>();

		for (ArrayList<String> list : worthList) {

			for (int i = 0; i < list.size(); i++) {

				if (worthDTOMap.containsKey(list.get(i))) {

					worthDTOMap.put(list.get(i), worthDTOMap.get(list.get(i)) + i);

				} else {

					worthDTOMap.put(list.get(i), i);
				}
			}
		}

		/**
		 * The value comparator
		 */
		ValueComparator valComp = new ValueComparator(worthDTOMap);

		/**
		 * The sorted TreeMap
		 */
		TreeMap<String, Integer> sortedDTOMap = new TreeMap<>(valComp);

		/**
		 * The entrySet iterator
		 */
		Iterator<Entry<String, Integer>> it = sortedDTOMap.entrySet().iterator();

		while (it.hasNext()) {

			sortedDTOIdList.add(it.next().getKey());
		}

		return sortedDTOIdList;
	}

	/**
	 * Get the DTO to be processed at iteration 
	 * // TODO: manage linked DTOs
	 * 
	 * @param pSId
	 * @param firstGuess
	 * @param domainSize - the size of the scheduling domain
	 * @param varCount - the variable counter
	 * @param varCountList - the list of variable counters
	 * @return
	 * @throws Exception
	 */
	private SchedDTO getIterDTO(Long pSId, boolean firstGuess, int domainSize, 
			int varCount, ArrayList<Integer> varCountList) throws Exception {

		/**
		 * The scheduled DTO at iteration
		 */
		SchedDTO iterDTO = new SchedDTO();

		if (RequestChecker.hasEquivDTO(pSId, schedDTODomainMap.get(pSId)
				.get(varCount).get(0).getARId())) {

			// Select valid EquivalentDTO (only for the first guess of the incoming AR)
			if (firstGuess) {
			
				iterDTO = selectValidEquivDTO(pSId, firstGuess, varCount);
			
			} else {
			
				iterDTO = null;
			}

		} else if (RequestChecker.hasLinkedDTO(pSId, schedDTODomainMap.get(pSId)
				.get(varCount).get(0).getDTOId())) {
			
			// Select valid DTO Pair
			iterDTO = selectValidPairDTO(pSId, firstGuess, domainSize, varCount, varCountList);
			
		} else {

			// Select valid DTO
			iterDTO = selectValidDTO(pSId, firstGuess, varCount);
		}
		
		return iterDTO;
	}

	/**
	 * Handle the linked DTOs of the incoming AR
	 * TODO: Updated on 26/07/2022 for DI2S offline management
	 * @param pSId - the Planning Session Id
	 * @param domainSize - the size of the scheduling domain
	 * @param inSchedDTOList - the list of intial scheduling DTOs
	 * @throws Exception 
	 */
	private boolean planLinkedDTOs(Long pSId, int domainSize, ArrayList<SchedDTO> inSchedDTOList) throws Exception {
				
		/**
		 * Instance handlers
		 */
		SessionScheduler sessionScheduler = new SessionScheduler();
		
		/**
		 * The planning boolean
		 */
		boolean planned = true;
		
		/**
		 * The linked DTO list
		 */
		ArrayList<SchedDTO> linkSchedDTOList = new ArrayList<SchedDTO>();
	
		for (SchedDTO inSchedDTO : inSchedDTOList) {
			
			if (RulesPerformer.getPlannedDTOIds(pSId).contains(inSchedDTO.getDTOId())) {
				
				// Add linked DTOs
				linkSchedDTOList.add(inSchedDTO);

				if (inSchedDTO.getLinkDtoIdList() != null && !inSchedDTO.getLinkDtoIdList().isEmpty()) {
	
					logger.info("Handle DTO with link: " + inSchedDTO.getDTOId());
									
					if (RequestChecker.isLinkDTOScheduled(pSId, inSchedDTO)) {
						
						return true;
					
					} else {
					

					}
				}
			}
			
			// Added on 26/07/2022 -----
			
			if (EquivDTOHandler.di2sLinkedIdsMap.get(pSId) != null
					&& EquivDTOHandler.di2sLinkedIdsMap.get(pSId).containsKey(inSchedDTO.getDTOId())
					&& PRListProcessor.schedDTOMap.get(pSId).containsKey(
							EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(inSchedDTO.getDTOId()))) {
				
				logger.debug("Added linked DTO: " + EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(inSchedDTO.getDTOId()));
				
				// Add linked DTOs
				linkSchedDTOList.add(PRListProcessor.schedDTOMap.get(pSId).get(
						EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(inSchedDTO.getDTOId())));
			}
			
			// -----
		}

		// Update scheduling solution
		updateSchedARIndex(pSId, 0);
		
		updatePlan(pSId, domainSize, planned);
		
		// Set plan statuses
		sessionScheduler.setPlanStatuses(pSId, schedSol, linkSchedDTOList);
			
		return planned;
	}
	
	/**
	 * Change the linked DTOs in case of previous link rejection
	 *
	 * @param pSId - the Planning Session Id
	 * @param domainSize - the size of the scheduling domain
	 * @param inSchedDTOList - the list of initial scheduling DTOs
	 * @throws Exception 
	 */
	private boolean changeLinkedDTOs(Long pSId, int domainSize, ArrayList<SchedDTO> inSchedDTOList) throws Exception {
				
		/**
		 * Instance handlers
		 */
		SessionScheduler sessionScheduler = new SessionScheduler();
		
		boolean planned = false;
		
		for (SchedDTO inSchedDTO : inSchedDTOList) {

			if (inSchedDTO.getLinkDtoIdList() != null && inSchedDTO.getLinkDtoIdList().isEmpty()) {

				logger.info("Handle DTO with link: " + inSchedDTO.getDTOId());
				
				/**
				 * The linked scheduling DTO list
				 */
				ArrayList<SchedDTO> linkSchedDTOList = new ArrayList<SchedDTO>();
				
				if (RequestChecker.isLinkDTOScheduled(pSId, inSchedDTO)) {
					
//					logger.debug("Retract linked DTO: " + inSchedDTO.getLinkDtoId());
//					
//					// Retract linked DTO and try to plan others 					
//					rulesPerformer.retractDTOById(pSId, inSchedDTO.getLinkDtoId(), ReasonOfReject.deletedByCsps);
//						
//					// Add additional DTOs relevant to the linked AR
//					for (int i = 0; i < domainSize; i ++) {
//					
//						if (schedDTODomainMap.get(pSId).get(i).get(0).getARId().equals(
//								ObjectMapper.getSchedARId(inSchedDTO.getLinkDtoId()))) {
//						
//							ArrayList<SchedDTO> initSchedDTOList = new ArrayList<SchedDTO>();
//							
//							for (SchedDTO schedDTO : schedDTODomainMap.get(pSId).get(i)) {
//														
//								if (! initSchedDTOList.isEmpty() && schedDTO.getDTOId().equals(inSchedDTO.getLinkDtoId())) {
//							
//									linkSchedDTOList.add(initSchedDTOList.get(initSchedDTOList.size() - 1));
//									
//									// Plan DTOs relevant to the linked AR
//									if (!rulesPerformer.planSchedDTOList(pSId, linkSchedDTOList, true)) {
//										
//										planned = true;
//									}
//								}
//								
//								initSchedDTOList.add(schedDTO);
//							}
//						}
//					}
				}
				
				// Update scheduling solution
				updateSchedARIndex(pSId, 0);
				
				// Update plan
				updatePlan(pSId, domainSize, planned);
				
				// Set plan statuses
				sessionScheduler.setPlanStatuses(pSId, schedSol, linkSchedDTOList);
			}		
		}
		
		return planned;
	}
}
