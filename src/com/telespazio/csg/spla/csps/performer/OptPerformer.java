/**
*
* MODULE FILE NAME: OptPerformer.java
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
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.ontology.resourceData.DTO;
import com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.utils.IntMatrixCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SchedDTOTimeComparator;

import it.sistematica.spla.datamodel.core.enums.Category;
import it.sistematica.spla.datamodel.core.enums.PRMode;

/**
 * The Optimal-based Scheduling performer class for the ARs DTOs through
 * heuristic algorithms.
 */
public class OptPerformer {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(OptPerformer.class);

	/**
	 * The initial solution lists of DTOs
	 */
	private ArrayList<ArrayList<SchedDTO>> initSolList;

	/**
	 * The best solution of DTOs
	 */
	private ArrayList<SchedDTO> bestOptSol;

	/**
	 * The map of the partners probability
	 */
	private static HashMap<String, Double> partnerProbMap;

	/**
	 * Perform Optimization-based scheduling for the Unranked Requests. 
	 * // TODO: Test about previous tasks scheduling 
	 * // TODO: manage stereoscopic DTOs
	 *
	 * @param pSId
	 *            - the Planning Session Id
	 * @param unrankARList
	 *            - the list of unranked ARs
	 * @param cutOffTime
	 *            - the cut-off Time for the Optimal Scheduling
	 * @return
	 * @throws Exception
	 */
	public ArrayList<SchedDTO> performOHScheduling(Long pSId, ArrayList<SchedAR> unrankARList, 
			double cutoffTime) throws Exception {

		/**
		 * Instance handlers
		 */
		IntMatrixCalculator intMatrixCalculator = new IntMatrixCalculator();
		
		RulesPerformer rulesPerformer = new RulesPerformer();

		try {
		
			/**
			 * Initialize best optimal solution
			 */
			bestOptSol = new ArrayList<>();
	
			// 1.0. Delete DTOs outside the MH
			unrankARList = deleteOutMHDTOs(unrankARList, pSId);
	
			// 1.1. Delete Left DTOs 
			// Added on 15/11/2022 to avoid left unranked
			unrankARList = deleteLeftDTOs(unrankARList, pSId);

			// 1.2. Build Intersection Matrix
			intMatrixCalculator.buildARIntMatrix(pSId, unrankARList);
	
			// 1.3. Filter unranked DTOs, excluding DTOs when conflicting with
			// ranked ones
			ArrayList<SchedDTO> optDTOList = new ArrayList<>();
			
			logger.debug("Initialize the unranked AR List...");
			
			if (!unrankARList.isEmpty()) {
	
				for (int i = 0; i < unrankARList.size(); i++) {
					
					for (int j = 0; j < unrankARList.get(i).getDtoList().size(); j++) {
	
						/**
						 * The conflict boolean
						 */
						boolean rankConfl = false;
	
						/**
						 * The optimal scheduling DTO
						 */
						SchedDTO optDTO = unrankARList.get(i).getDtoList().get(j);
	
						for (int k = 0; k < SessionScheduler.schedDTOListMap.get(pSId).size(); k ++) {
	
							SchedDTO schedDTO = SessionScheduler.schedDTOListMap.get(pSId).get(k);
	
							if (IntMatrixCalculator.intDTOMatrixMap.get(pSId).get(schedDTO.getDTOId())
									.get(optDTO.getDTOId()) == 0) {
	
								rankConfl = true;
							}
						}
	
						if (!rankConfl) {
	
							// Add DTO
							optDTOList.add(unrankARList.get(i).getDtoList().get(j));
						}
					}
				}
		
				// 2.0 Configure Owner probabilities
				configOwnerProbs(pSId);

				/**
				 * The initial list of DTOList
				 */
				ArrayList<ArrayList<SchedDTO>> initSchedSolList = initOptScheduling(pSId, optDTOList);
	
				// 2.1.1 Compute the best initial solution
				computeBestOptSol(initSchedSolList);
	
				// if (bestOptSol.size() < unrankARList.size()) {
				//
				// // 2.2 Process Genetic Algorithm
				// logger.debug("Process Genetic Algorthm...");
				// processGA(initDTOListList, optDTOList);
				//
				// // 2.3 Validate solution
				// validateSol(pSId, unrankARList);
	
				// 2.4 Process Simulated Annealing (TODO: adapt: threshold, cooling,
				// cost function)
	
				// logger.debug("Process Simulated Annealing...");
				// processSA(pSId, unrankARList, cutoffTime); // TODO: refine
				// timeout and selARIndMap
	
				// }
	
				// 3.0. Validate optimal solution
				boolean isComplete = validateSol(pSId, unrankARList, cutoffTime);
				
				if (isComplete) {
					
					logger.info("The solution is valid and complete.");
				
				} else {
					
					logger.warn ("The solution is valid but NOT complete.");	
				}
	
				/**
				 * The list of excluded DTOs
				 */
				ArrayList<SchedDTO> exclDTOList = new ArrayList<SchedDTO>();

				/**
				 * Add excluded DTOs from initial unranked list
				 * // Update on 22/11/2022 to avoid Left unraked acquisitions
				 */
				for (SchedDTO optDTO : optDTOList) {
					
					/**
					 * The including boolean
					 */
					boolean isIncluded = false;
					
					/**
					 * The ignoring boolean
					 */
					boolean  isIgnored = false;
					
					for (SchedDTO schedDTO : bestOptSol) {
						
						if (optDTO.getLookSide().equalsIgnoreCase("Left")) {
							
							isIgnored = true;
							
							break;
						}
						
						if (optDTO.getARId().equals(schedDTO.getARId())) {
							
							isIncluded = true;
							
							break;		
						}
					}
						
					if (!isIncluded || isIgnored) {
						
						exclDTOList.add(optDTO);
					}
				}
							
				// Added on 12/04/2022 for cut-off management
				if (cutoffTime > 0) {
					
					// 4.0. Plan the excluded DTOs					
					logger.info("Try to replan excluded DTOs...");
					
					/**
					 * The available time for scheduling
					 */
					cutoffTime = (Configuration.optSchedTime
							- (new Date().getTime() - SessionActivator.planDateMap.get(pSId).getTime()));
					
					Date date =  new Date();
					
					for (int i = 0; i <  exclDTOList.size(); i ++) {
					
						if (new Date().getTime() - date.getTime() > cutoffTime) {
							
							rulesPerformer.planSchedDTO(pSId, exclDTOList.get(i));				

						} else {
							
							logger.info("Cut-off time " + Configuration.optSchedTime 
									+ " (ms) for the optimal scheduling is reached!");
							
							break;
						}
					}

					
				} else {
					
					logger.warn("Cut-off time reached for replanning.");
				}
			
				// 5.0. Set rejected DTOs
				rulesPerformer.setRejectedDTOs(pSId);
				
				// Clear intersection matrix
				IntMatrixCalculator.intDTOMatrixMap.get(pSId).clear();
			}
		
		} catch (Exception ex) {
			
			logger.error("Exception raised during the unranked optimization: " + ex.getStackTrace()[0].toString());				
			
			bestOptSol.clear();
		}

		logger.debug("A number of " + bestOptSol.size() + " DTOs is returned.");
		
		return bestOptSol;
	}

	/**
	 * Initialize the optimization based scheduling strategy for the unranked
	 * requests.
	 *
	 * @param pSId
	 * @param timedDTOList
	 *            - the timed list of the input DTOs
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<ArrayList<SchedDTO>> initOptScheduling(Long pSId, ArrayList<SchedDTO> timedDTOList)
			throws Exception {
		
		logger.debug("Perform Optimal Scheduling Initialization.");
		
		/**
		 * The iterations
		 */
		int iters = timedDTOList.size(); // TODO: TBD value wrt plan complexity?

		/**
		 * The initial solution list
		 */
		this.initSolList = new ArrayList<>();

		/**
	     * The initial solution
		 */	
		ArrayList<SchedDTO> initSol = new ArrayList<>();
		
		/**
		 * The list of AR Ids
		 */
		ArrayList<String> aRIdList = new ArrayList<>();

		/**
		 * The counter
		 */
		int count = 0;

		for (int iter = 0; iter < iters; iter++) {

			for (int i = 0; i < timedDTOList.size(); i++) {

				if ((count + i) == timedDTOList.size()) {

					count = -i;
				}

				/**
				 * The set of conflicting DTOs
				 */
				ArrayList<SchedDTO> conflDTOSet = new ArrayList<>();

				if (!aRIdList.contains(timedDTOList.get(count + i).getARId())) {

					if (! checkSolConflict(pSId, initSol, timedDTOList.get(count + i))) {

						conflDTOSet.add(timedDTOList.get(count + i));
					}
				}

				if (!conflDTOSet.isEmpty()) {

					for (int j = 0; j < timedDTOList.size(); j++) {

						if (!IntMatrixCalculator.intDTOMatrixMap.get(pSId).isEmpty()) {

							if (IntMatrixCalculator.intDTOMatrixMap.get(pSId)
									.get(timedDTOList.get(count + i).getDTOId())
									.containsKey(timedDTOList.get(j).getDTOId())) {

								if (IntMatrixCalculator.intDTOMatrixMap.get(pSId)
										.get(timedDTOList.get(count + i).getDTOId())
										.get(timedDTOList.get(j).getDTOId()) == 0) {

									if (!aRIdList.contains(timedDTOList.get(j).getARId())) {

										if (!checkSolConflict(pSId, initSol, timedDTOList.get(j))) {
											
											conflDTOSet.add(timedDTOList.get(j));
										}
									}
								}
							}
						}
					}

					// Select a DTO according to its partner probability
					initSol.add(getProbSchedDTO(pSId, (ArrayList<SchedDTO>) conflDTOSet.clone()));

					// initSol.add(conflDTOSet.get(0));

					aRIdList.add(initSol.get(initSol.size() - 1).getARId());

					conflDTOSet.clear();
				}
			}

			count = iter + 1;

			aRIdList.clear();

			this.initSolList.add((ArrayList<SchedDTO>) initSol.clone());

			initSol.clear();
		}

		return this.initSolList;

	}

	/**
	 * Check conflict of a timedDTO in the built solution
	 * 
	 * @param pSId
	 * @param initSol
	 * @param timedDTO
	 * @return
	 */
	private boolean checkSolConflict(Long pSId, ArrayList<SchedDTO> initSol, SchedDTO timedDTO) {
		
		/**
		 * The conflict boolean
		 */ 
		boolean confl = false;

		if (!initSol.isEmpty()) {

			for (int k = 0; k < initSol.size(); k++) {

				// Check conflict
				if (IntMatrixCalculator.intDTOMatrixMap.get(pSId).get(initSol.get(k).getDTOId())
						.containsKey(timedDTO.getDTOId())) {

					if (IntMatrixCalculator.intDTOMatrixMap.get(pSId).get(initSol.get(k).getDTOId())
							.get(timedDTO.getDTOId()) == 0) {

						// Set conflict
						confl = true;
					}
				}
			}
		}

		return confl;
	}

	// /**
	// * Process the Genetic Algorithm
	// * @param initDTOListList
	// */
	// @SuppressWarnings("unchecked")
	// private ArrayList<SchedDTO> processGA(ArrayList<ArrayList<SchedDTO>>
	// initDTOListList,
	// ArrayList<SchedDTO> unrankARList) {
	//
	// int maxIter = 100; // TODO: TBD
	//
	// double crossFac = 1;
	// double mutFac = 0.1;
	// double elitFac = 0.1;
	//
	// for (int gaIter = 0; gaIter < maxIter; gaIter ++) {
	//
	// // The list of solutions at iteration
	// ArrayList<ArrayList<SchedDTO>> gaSolList = new
	// ArrayList<ArrayList<SchedDTO>>();
	//
	// // 1.1. Set crossover
	// ArrayList<SchedDTO> sol1 = new ArrayList<SchedDTO>();
	// ArrayList<SchedDTO> sol2 = new ArrayList<SchedDTO>();
	//
	// for (int i = 0; i < initDTOListList.size(); i ++) {
	//
	// if (new Random(new Random().nextLong()).nextDouble() < crossFac) {
	//
	// int randInt1 = new Random(new
	// Random().nextLong()).nextInt(initDTOListList.size());
	//
	// int randInt2 = new Random(new
	// Random().nextLong()).nextInt(initDTOListList.size());
	//
	// sol1 = initDTOListList.get(randInt1);
	//
	// sol2 = initDTOListList.get(randInt2);
	//
	// // 1.1.2. Choose pair of solutions to be crossed
	// if (! sol1.isEmpty() && ! sol2.isEmpty()) {
	//
	// gaSolList.addAll(crossSolutions(sol1, sol2));
	// }
	// }
	// }
	//
	// // 1.2. Set mutation
	// ArrayList<SchedDTO> mutSol = new ArrayList<SchedDTO>();
	//
	// for (int i = 0; i < gaSolList.size() * mutFac; i ++) {
	//
	// if (new Random(new Random().nextLong()).nextDouble() < mutFac) {
	//
	// mutSol = gaSolList.get(i);
	//
	// if (! mutSol.isEmpty()) {
	//
	// gaSolList.set(i, mutateSolution(mutSol, unrankARList));
	// }
	// }
	//
	// }
	//
	// // 1.3. Set elitism technique
	// eliteSolutions((ArrayList<ArrayList<SchedDTO>>) gaSolList.clone());
	// // TODO change
	// initDTOListList.add((ArrayList<SchedDTO>) bestOptSol.clone());
	//
	// // 1.4 Clear solution list
	// gaSolList.clear();
	// }
	//
	// return bestOptSol;
	// }
	//
	//
	// /**
	// * Single point crossover
	// * @param dtoList1
	// * @param dtoList2
	// * @return
	// */
	// private ArrayList<ArrayList<SchedDTO>> crossSolutions(ArrayList<SchedDTO>
	// sol1, ArrayList<SchedDTO> sol2) {
	//
	// // The output solutions
	// ArrayList<ArrayList<SchedDTO>> outSolList = new
	// ArrayList<ArrayList<SchedDTO>>();
	// outSolList.add(0, new ArrayList<SchedDTO>());
	// outSolList.add(1, new ArrayList<SchedDTO>());
	//
	// // Sort DTOs by time
	// Collections.sort(sol1, new DTOTimeComparator());
	// Collections.sort(sol2, new DTOTimeComparator());
	//
	// double[] boundTimes = {sol1.get(0).getStartTime().getTime(),
	// sol2.get(sol2.size() - 1).getStopTime().getTime()};
	//
	// // Select the crossover time
	// double crossFac = new Random(new Random().nextLong()).nextDouble();
	//
	// double crossTime = boundTimes[0] + crossFac * (boundTimes[1] -
	// boundTimes[0]);
	//
	// ArrayList<ArrayList<String>> aRIdList = new
	// ArrayList<ArrayList<String>>();
	// aRIdList.add(0, new ArrayList<String>());
	// aRIdList.add(1, new ArrayList<String>());
	//
	// for (int i = 0; i < sol1.size(); i++) {
	//
	// if (sol1.get(i).getStartTime().getTime() < crossTime
	// && sol1.get(i).getStopTime().getTime() > crossTime) {
	//
	// sol1.remove(i);
	//
	// i--;
	//
	// } else if (sol1.get(i).getStopTime().getTime() <= crossTime) {
	//
	// outSolList.get(0).add(sol1.get(i));
	//
	// aRIdList.get(0).add(sol1.get(i).getARId());
	//
	// } else if (sol1.get(i).getStartTime().getTime() > crossTime) {
	//
	// outSolList.get(1).add(sol1.get(i));
	//
	// aRIdList.get(1).add(sol1.get(i).getARId());
	//
	// }
	// }
	//
	// for (int i = 0; i < sol2.size(); i++) {
	//
	// if (sol2.get(i).getStartTime().getTime() < crossTime
	// && sol2.get(i).getStopTime().getTime() > crossTime) {
	//
	// sol2.remove(i);
	//
	// i--;
	//
	// } else if (sol2.get(i).getStopTime().getTime() <= crossTime
	// && ! aRIdList.get(1).contains(sol2.get(i).getARId())) {
	//
	// outSolList.get(1).add(sol2.get(i));
	//
	// } else if (sol2.get(i).getStartTime().getTime() > crossTime
	// && ! aRIdList.get(0).contains(sol2.get(i).getARId())) {
	//
	// outSolList.get(0).add(sol2.get(i));
	//
	// }
	// }
	//
	// return outSolList;
	// }
	//
	// /**
	// * Perform the mutation of the solution
	// * @param sol
	// * @param unrankARList
	// * @return
	// */
	// private ArrayList<SchedDTO> mutateSolution(ArrayList<SchedDTO> sol,
	// ArrayList<SchedDTO> unrankARList) {
	//
	// Collections.sort(sol, new DTOTimeComparator());
	//
	// int randInt = new Random(new Random().nextLong()).nextInt(sol.size());
	//
	// int randPreInt, randPostInt;
	//
	// if (randInt == 0) {
	//
	// randPreInt = 0;
	//
	// } else {
	//
	// randPreInt = randInt - 1;
	// }
	//
	// if (randInt == sol.size() - 1) {
	//
	// randPostInt = 0;
	//
	// } else {
	//
	// randPostInt = randInt + 1;
	// }
	//
	// for (int i = 0; i < unrankARList.size(); i ++) {
	//
	// if (IntMatrixHandler.intMatrix.get(sol.get(randInt).getDtoId())
	// .containsValue(unrankARList.get(i).getDtoId())) {
	//
	// if (IntMatrixHandler.intMatrix.get(sol.get(randInt).getDtoId())
	// .get(unrankARList.get(i).getDtoId()).equals(0)
	// && !IntMatrixHandler.intMatrix.get(sol.get(randPreInt).getDtoId())
	// .get(unrankARList.get(i).getDtoId()).equals(0)
	// && !IntMatrixHandler.intMatrix.get(sol.get(randPostInt).getDtoId())
	// .get(unrankARList.get(i).getDtoId()).equals(0)) {
	//
	// sol.set(randInt, unrankARList.get(i));
	//
	// break;
	// }
	// }
	// }
	//
	// return sol;
	// }
	//
	//
	/**
	 * Set the best solution according to the cost function objective 
	 * TODO: TBD objective function factors (the number of DTOs the only factor?)
	 *
	 * @param solList
	 */
	@SuppressWarnings("unchecked")
	private void computeBestOptSol(ArrayList<ArrayList<SchedDTO>> schedSolList) throws Exception {

		logger.debug("Compute Optimal Scheduling best solution.");
		
		/**
		 * The maximum solution size
		 */ 
		int maxSolSize = 0;

		for (int i = 0; i < schedSolList.size(); i++) {

			if (schedSolList.get(i).size() > maxSolSize) {

				bestOptSol = (ArrayList<SchedDTO>) schedSolList.get(i).clone();

				maxSolSize = bestOptSol.size();
			}
		}
		
		logger.debug("Best solution size equals to: " + maxSolSize);
	}

//	/**
//	 * Schedule the rejected DTOs by generation
//	 *
//	 * @param pSId
//	 * @param unrankList
//	 * @throws Exception
//	 */
//	@SuppressWarnings("unchecked")
//	private void scheduleRejDTOs(Long pSId, ArrayList<SchedAR> unrankList) throws Exception {
//
//		for (SchedAR schedAR : unrankList) {
//
//			/**
//			 * The scheduled status
//			 */
//			boolean scheduled = false;
//
//			for (SchedDTO schedDTO : schedAR.getDtoList()) {
//
//				for (SchedDTO optDTO : (ArrayList<SchedDTO>) bestOptSol.clone()) {
//
//					if (optDTO.getDTOId().equals(schedDTO.getDTOId())) {
//
//						scheduled = true;
//
//						logger.info("AR : " + schedAR.getARId() +
//						" has been scheduled by Optimization-based algorithm.");
//						
//						break;
//					}
//				}
//			}
//
//			if (!scheduled) {
//
//				logger.info("AR : " + schedAR.getARId() + " is tried to be re-scheduled.");
//
//				// TODO: manage linked DTOs
//
//				if (RequestChecker.hasEquivDTO(pSId, schedAR.getARId())) {
//
//					selectValidEquivDTO(pSId, false, schedAR.getDtoList());
//
//				} else {
//
//					selectValidDTO(pSId, schedAR.getDtoList());
//				}
//			}
//		}
//	}

	// /**
	// * Process the Simulated Annealing
	// * // TODO: check the SA processing and timeouts!
	// *
	// * @param pSId
	// * @param unrankARList
	// * @param saTimeout
	// * @throws Exception
	// */
	// @SuppressWarnings("unchecked")
	// private void processSA(Long pSId, ArrayList<SchedAR> unrankARList, double
	// saTimeout) throws Exception {
	//
	// /**
	// * Instance handlers
	// */
	// RankPerformer rankPerformer = new RankPerformer();
	//
	// // 1.0 Perform Simulated Annealing while timeout is reached
	// Date initDate = new Date();
	// while (new Date().getTime() - initDate.getTime() < saTimeout) {
	//
	// /**
	// * The domain of the scheduled DTOs in the solution
	// */
	// ArrayList<ArrayList<SchedDTO>> schedDTODomain = new
	// ArrayList<ArrayList<SchedDTO>>();
	//
	// /**
	// * The domain of the rejected DTOs from the solution
	// */
	// ArrayList<ArrayList<SchedDTO>> rejDTODomain = new
	// ArrayList<ArrayList<SchedDTO>>();
	//
	// /**
	// * The set of the first rejected DTOs
	// */
	// ArrayList<SchedDTO> rejDTOSet = new ArrayList<SchedDTO>();
	//
	// logger.info("Perform conflict resolution for the unranked ARs set.");
	//
	// for (int i = 0; i < unrankARList.size(); i ++) {
	//
	// /**
	// * The scheduling boolean
	// */
	// boolean schedBool = false;
	//
	// for (int j = 0; j < unrankARList.get(i).getDtoList().size(); j ++) {
	//
	// for (SchedDTO schedDTO : bestOptSol) {
	//
	// if
	// (unrankARList.get(i).getDtoList().get(j).getDTOId().equals(schedDTO.getDTOId()))
	// {
	//
	// schedDTODomain.add(unrankARList.get(i).getDtoList());
	//
	// schedBool = true;
	// }
	// }
	// }
	//
	// if (! schedBool) {
	//
	// rejDTOSet.add(unrankARList.get(i).getDtoList().get(0));
	//
	// rejDTODomain.add(unrankARList.get(i).getDtoList());
	// }
	// }
	//
	// // 1.1 Select a rejected AR according to the BIC quota part of each
	// partner
	// ArrayList<SchedDTO> rejDTOList = getProbDTOList(pSId, rejDTOSet,
	// rejDTODomain);
	//
	// // 1.2 Compute a restricted CBJ for the initially excluded ARs
	// bestOptSol = rankPerformer.performRevCBJ(pSId, rejDTOList,
	// schedDTODomain,
	// (ArrayList<SchedDTO>) bestOptSol.clone(), saTimeout / (double)
	// rejDTOList.size());
	//
	// logger.debug("DTO size of the optimal solution: " + bestOptSol.size());
	//
	// if (isOptComplete(bestOptSol, unrankARList)) {
	//
	// break;
	// }
	// }
	// }

	// /**
	// * Check if the optimal solution is complete
	// * @param bestOptSol
	// * @param unrankARList
	// */
	// private boolean isOptComplete(ArrayList<SchedDTO> bestOptSol,
	// ArrayList<SchedAR> unrankARList) {
	//
	// /**
	// * The output boolean
	// */
	// boolean isComplete = false;
	//
	// /**
	// * The number of optimally scheduled DTOs
	// */
	// int scheduled = 0;
	//
	// for (SchedDTO schedDTO : bestOptSol) {
	//
	// for (SchedAR unrankAR : unrankARList) {
	//
	// for (SchedDTO unrankDTO : unrankAR.getDtoList()) {
	//
	// if (schedDTO.getDTOId().equals(unrankDTO.getDTOId())) {
	//
	// scheduled ++;
	//
	// break;
	// }
	// }
	// }
	// }
	//
	// if (scheduled == unrankARList.size()) {
	//
	// isComplete = true;
	// }
	//
	// return isComplete;
	// }

//	/**
//	 * Select a valid DTO according to the inference process within the BRM
//	 * scheduling rules
//	 *
//	 * @param pSId
//	 *            - the current planning session Id
//	 * @param newSchedDTOList
//	 *            - the DTOlist to be scheduled
//	 * @return the selected DTO if consistent
//	 * @throws Exception
//	 */
//	@SuppressWarnings("unchecked")
//	private void selectValidDTO(Long pSId, ArrayList<SchedDTO> newSchedDTOList) throws Exception {
//
//		/**
//		 * The output DTO
//		 */
//		SchedDTO selSchedDTO = null;
//		
//		/**
//		 * Instance handler
//		 */
//		RulesPerformer rulesPerformer = new RulesPerformer();
//
//		// Retract conflicting DTO List
//		logger.info("Retract DTOs relevant to conflicting AR: " + newSchedDTOList.get(0).getARId());
//
//		rulesPerformer.retractDTOList(pSId, (ArrayList<SchedDTO>) newSchedDTOList.clone(), ReasonOfReject.deletedByCsps);
//
//		// While new DTOs exist
//		for (int i = 0; i < newSchedDTOList.size(); i++) {
//
//			/**
//			 * The selected DTO
//			 */
//			selSchedDTO = newSchedDTOList.get(i);
//
//			// Select best element of DTO domain
//			logger.info("Selected DTO at iteration: " + selSchedDTO.getDTOId());
//
//			// consistency boolean
//			boolean consistency = true;
//
//			// Check consistency of the scheduling rules within BRM
//			if (!rulesPerformer.planDTO(pSId, selSchedDTO, true)) {
//
//				// Update consistency
//				consistency = false;
//								
//				selSchedDTO = null;
//			}
//
//			if (consistency) {
//
//				logger.info("Selected AR DTO " + selSchedDTO.getDTOId() + " is consistent.");
//
//				bestOptSol.add(selSchedDTO);
//
//				break;
//			}
//		}
//
//		if (selSchedDTO == null) {
//		
//			logger.debug("DTOs of selected AR are inconsistent.");
//		}
//	}
//
//	/**
//	 * Select a valid Equivalent DTO according to the inference process within the
//	 * BRM scheduling rules
//	 *
//	 * @param pSId
//	 *            - the current planning session Id
//	 * @param firstGuess
//	 *            - the first guess boolean
//	 * @param schedARList
//	 *            - the variable counter
//	 * @return the first DTO of the equivalent one, if consistent
//	 * @throws Exception
//	 */
//	@SuppressWarnings("unchecked")
//	private void selectValidEquivDTO(Long pSId, boolean firstGuess, ArrayList<SchedDTO> schedDTOList)
//			throws Exception {
//
//		/**
//		 * The output selected DTO
//		 */
//		SchedDTO selSchedDTO = null;
//		
//		/**
//		 * Instance handler
//		 */
//		RulesPerformer rulesPerformer = new RulesPerformer();
//
//		// Retract conflicting DTO List
//		logger.info("Retract DTOs relevant to AR: " + schedDTOList.get(0).getARId());
//		rulesPerformer.retractDTOList(pSId, (ArrayList<SchedDTO>) schedDTOList.clone(), ReasonOfReject.deletedByCsps);
//
//		logger.info("Selected Equivalent DTO at iteration for AR : " + schedDTOList.get(0).getARId());
//
//		/**
//		 * The list of scheduled DTOs
//		 */
//		ArrayList<SchedDTO> dtoList = new ArrayList<>();
//
//		// While new DTOs exist
//		for (int i = 0; i < schedDTOList.size(); i++) {
//
//			/**
//			 * The selected DTO
//			 */
//			selSchedDTO = schedDTOList.get(i);
//
//			dtoList.add(selSchedDTO);
//		}
//
//		// consistency boolean
//		boolean consistency = true;
//
//		// Check consistency of the scheduling rules within BRM
//		if (!rulesPerformer.planDTOList(pSId, dtoList, true)) {
//
//			// Update consistency
//			consistency = false;
//			
//			selSchedDTO = null;
//		}
//
//		if (consistency) {
//
//			logger.info("Selected Equivalent DTO at iteration for AR : " + schedDTOList.get(0).getARId()
//					+ " is consistent.");
//
//			bestOptSol.addAll((ArrayList<SchedDTO>) dtoList.clone());
//		
//		} else {
//
//			logger.debug("DTOs of selected AR are inconsistent.");
//		}
//	}

	// /**
	// * Get the DTO list to be selected according to the involved partner
	// probabilities
	// *
	// * @param pSId
	// * @param conflDTOSet
	// * @param conflDTODomain
	// * @return
	// * @throws Exception
	// */
	// private ArrayList<SchedDTO> getProbDTOList(Long pSId, ArrayList<SchedDTO>
	// conflDTOSet,
	// ArrayList<ArrayList<SchedDTO>> conflDTODomain) throws Exception {
	//
	// /**
	// * The output list of scheduled DTOs
	// */
	// ArrayList<SchedDTO> probDTOList = new ArrayList<SchedDTO>();
	//
	// /**
	// * The conflict probabilities
	// */
	// double[] conflProbs = new double[conflDTOSet.size()];
	//
	// double totProb = 0;
	//
	// for (int i = 0; i < conflDTOSet.size(); i ++) {
	//
	// conflProbs[i] = partnerProbMap.get(SessionActivator
	// .getSchedAROwnerIdList(pSId, conflDTOSet.get(i).getARId()).get(0)); //
	// TODO: add subscribers impact
	//
	// totProb += conflProbs[i];
	// }
	//
	// double prob = new Random(new Random().nextLong()).nextDouble();
	//
	// double tempProb = 0;
	//
	// for (int i = 0; i < conflProbs.length; i ++) {
	//
	// tempProb += conflProbs[i] / totProb;
	//
	// if (prob < tempProb) {
	//
	// probDTOList = conflDTODomain.get(i);
	//
	// }
	// }
	//
	// return probDTOList;
	// }

	
	/**
	 * Configure Owner Probabilities based on BIC calculations
	 *
	 *
	 * @param pSId
	 */
	private void configOwnerProbs(Long pSId) throws Exception {

		partnerProbMap = new HashMap<>();

		// Compute Defense total quota
		int unusedDefPreBIC = 0;
		int unusedDefRtnBIC = 0;

		// Compute Civilian total quota
		int unusedCivPreBIC = 0;
		int unusedCivRtnBIC = 0;

		// Get partner BIC Map

		for (int i = 0; i < SessionActivator.ownerListMap.get(pSId).size(); i++) {

			if (SessionActivator.ownerListMap.get(pSId).get(i).getCatalogOwner().getUserCategory()
					.equals(Category.Defence)) {

				unusedDefPreBIC += SessionScheduler.ownerBICMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId)
						.get(i).getCatalogOwner().getOwnerId())[0];
				unusedDefRtnBIC += SessionScheduler.ownerBICMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId)
						.get(i).getCatalogOwner().getOwnerId())[1];

			} else {

				unusedCivPreBIC += SessionScheduler.ownerBICMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId)
						.get(i).getCatalogOwner().getOwnerId())[0];
				unusedCivRtnBIC += SessionScheduler.ownerBICMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId)
						.get(i).getCatalogOwner().getOwnerId())[1];
			}
		}

		/**
		 * The total quota of BIC
		 */
		double totQuotaBIC = unusedDefPreBIC + unusedDefRtnBIC + unusedCivPreBIC + unusedCivRtnBIC;
		
		if (totQuotaBIC == 0) {
			
			totQuotaBIC = 1.0;
		}

		// Compute Defense quota probability
		for (int i = 0; i < SessionActivator.ownerListMap.get(pSId).size(); i++) {

			/**
			 * The probability
			 */ 
			double prob = (SessionScheduler.ownerBICMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId)
					.get(i).getCatalogOwner().getOwnerId())[0]
					+ SessionScheduler.ownerBICMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId)
							.get(i).getCatalogOwner().getOwnerId())[1]) / totQuotaBIC;

			// Add probability		
			partnerProbMap.put(SessionActivator.ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId(), prob);
		}

		// Compute Civilian quota probability
		for (int i = 0; i < SessionActivator.ownerListMap.get(pSId).size(); i++) {

			/**
			 * The choosing probability
			 */
			double prob = (SessionScheduler.ownerBICMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId)
					.get(i).getCatalogOwner().getOwnerId())[0]
					+ SessionScheduler.ownerBICMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId)
							.get(i).getCatalogOwner().getOwnerId())[1]) / totQuotaBIC;

			// Add partner probability		
			partnerProbMap.put(SessionActivator.ownerListMap.get(pSId)
					.get(i).getCatalogOwner().getOwnerId(), prob);
		}
	}

	/**
	 * Get the DTO to be selected according to the involved partner probabilities
	 * // TODO:  add subscribers impact
	 * 
	 * @param pSId
	 * @param conflDTOSet
	 * @return
	 * @throws Exception
	 */
	private SchedDTO getProbSchedDTO(Long pSId, ArrayList<SchedDTO> conflDTOSet) throws Exception {

		/**
		 * The outputDTO
		 */
		SchedDTO probSchedDTO = null;
		
		/**
		 * The conflict probability array
		 */
		double[] conflProbs = new double[conflDTOSet.size()];

		/**
		* The total probability
		*/
		double totProb = 0;

		for (int i = 0; i < conflDTOSet.size(); i++) {

			conflProbs[i] = partnerProbMap
					.get(getSchedAROwnerIdList(pSId, conflDTOSet.get(i).getARId()).get(0));

			totProb += conflProbs[i];
		}

		double prob = new Random(new Random().nextLong()).nextDouble();

		double tempProb = 0;

		for (int i = 0; i < conflProbs.length; i++) {

			tempProb += conflProbs[i] / totProb;

			if (prob < tempProb) {

				probSchedDTO = conflDTOSet.get(i);
				
				break;

			}
		}

		return probSchedDTO;
	}

	/**
	 * Delete DTOs outside the Mission Horizon
	 *
	 * @param unrankARList
	 * @return the updated ranked list of ARs
	 */
	private ArrayList<SchedAR> deleteOutMHDTOs(ArrayList<SchedAR> unrankARList, Long pSId) {

		// Delete DTO outside MH
		for (int i = 0; i < unrankARList.size(); i++) {

			for (int j = 0; j < unrankARList.get(i).getDtoList().size(); j++) {

				if (! RequestChecker.isInsideMH(pSId, unrankARList.get(i).getDtoList().get(j))) {

					// Remove DTO
					unrankARList.get(i).getDtoList().remove(j);

					// reset counter
					j--;
				}
			}

			if (unrankARList.get(i).getDtoList().size() == 0) {

				// Remove DTO
				unrankARList.remove(i);

				// reset counter
				i--;
			}
		}

		return unrankARList;
	}
	
	/**
	 * Delete DTOs outside the Mission Horizon
	 * // Updated on 11/11/2022 to avoid Left acquisitions
	 * @param unrankARList
	 * @return the updated ranked list of ARs
	 */
	private ArrayList<SchedAR> deleteLeftDTOs(ArrayList<SchedAR> unrankARList, Long pSId) {

		// Delete DTO outside MH
		for (int i = 0; i < unrankARList.size(); i++) {

			for (int j = 0; j < unrankARList.get(i).getDtoList().size(); j++) {

				if (unrankARList.get(i).getDtoList().get(j).getLookSide().equalsIgnoreCase("Left")) {

					// Remove DTO
					unrankARList.get(i).getDtoList().remove(j);

					// reset counter
					j--;
				}
			}

			if (unrankARList.get(i).getDtoList().size() == 0) {

				// Remove DTO
				unrankARList.remove(i);

				// reset counter
				i--;
			}
		}

		return unrankARList;
	}

	/**
	 * Validate the scheduled solution
	 * // TODO: finalize validation
	 *
	 * @param pSId
	 * @param unrankARList
	 * @param cutOffTime
	 */
	private boolean validateSol(Long pSId, ArrayList <SchedAR> unrankARList, double cutoffTime) throws Exception {

		logger.debug("Validate Optimal Scheduling solution...");
		
		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();
		
		/**
		 * The output boolean
		 */
		boolean planned = true;
	
		/**
		 * The list of scheduled unranked DTOs
		 */
		ArrayList<SchedDTO> unrankSchedDTOList = new ArrayList<SchedDTO>();
				
		if (bestOptSol != null && bestOptSol.isEmpty()) {
			
			// Order best optimal solution by time
			Collections.sort(bestOptSol, new SchedDTOTimeComparator());
		
		} else {
			
			logger.debug("Empty best solution found.");
		}
			
		
		// Order updated best optimal solution by time
		Collections.sort(bestOptSol, new SchedDTOTimeComparator());
		
		// 2.0. Plan optimal solution
		planned = planOptSol(pSId, bestOptSol, unrankSchedDTOList, cutoffTime);
		
		/**
		 * The list of left DTOs
		 */
		ArrayList<SchedDTO> leftDTOList = new ArrayList<SchedDTO>();
		
		for (SchedAR schedAR : unrankARList) {
		
			for (SchedDTO schedDTO : bestOptSol) {
				
				if (! schedAR.getARId().equals(schedDTO.getARId())) {
					
					leftDTOList.add(schedAR.getDtoList().get(0));
				}
			}
		}
		
//		// 3.0. Validate left DTOs not included in the best solution	
//		validOptSol(pSId, leftDTOList, unrankSchedDTOList);	

		// 4.0. Update optimal solution
		bestOptSol = rulesPerformer.getAcceptedDTOs(pSId);
				
		// 4.1. Order best solution by time
		Collections.sort(bestOptSol, new SchedDTOTimeComparator());
		
		logger.info("A final best optimal solution of: " + bestOptSol.size() 
			+ " DTOs is scheduled");

		return planned;
	}
	
	/**
	 * Validate optimal solution
	 * 
	 * @param pSId
	 * @param bestOptSol
	 * @param unrankSchedDTOList
	 * @param cutoffTime
	 * @return
	 * @throws Exception 
	 */
	private boolean planOptSol(Long pSId, ArrayList<SchedDTO> schedSol, 
			ArrayList<SchedDTO> unrankSchedDTOList, double cutoffTime) throws Exception {
		
		/**
		 * The planned boolean
		 */
		boolean planned = true;
		
		/**
		 * Instance handler
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();
		
		/**
		 * The list of No Standard DTOs
		 */
		ArrayList<SchedDTO> noStdDTOList = new ArrayList<SchedDTO>();

		ArrayList<String> noStdDTOIdList = new ArrayList<String>();
		
		long dateTime = new Date().getTime();
		
		// 1.0. Plan best solution
		for (int i = 0; i < schedSol.size(); i ++) {
		
			// Added on 29/03/2022 for cut-off management
			double spentTime =  new Date().getTime() - dateTime;
			
			if (spentTime > cutoffTime) {
				
				logger.info("Cut-off time " + Configuration.optSchedTime 
						+ " (ms) for the optimal scheduling is reached!");
				
				break;
			}
			
			logger.debug("Plan unranked DTO: " + bestOptSol.get(i).getDTOId());
			
			/**
			 * The scheduled DTO
			 */
			SchedDTO schedDTO = schedSol.get(i);
			
			if (RequestChecker.hasEquivDTO(pSId, schedDTO.getARId())) {
					
				// 2.0. check inserting conditions
				if (schedDTO.getPRMode().equals(PRMode.Theatre)
						|| schedDTO.getPRMode().equals(PRMode.Experimental)
						|| schedDTO.getPRMode().equals(PRMode.DI2S)
						&& ! noStdDTOIdList.contains(schedDTO.getDTOId())) {
					
					/**
					 * The list of scheduling DTOs
					 */
					ArrayList<SchedDTO> equivDTOList = ObjectMapper.parseDMToSchedDTOList(pSId, schedDTO.getARId(), 
							PRListProcessor.aRSchedIdMap.get(pSId).get(schedDTO.getARId()).getDtoList(), 
							PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getUserList()
							.get(0).getAcquisitionStationIdList(), false);
				
					/**
					 * The Equivalent DTO
					 */
					EquivalentDTO equivDTO = ObjectMapper.parseSchedToBRMEquivDTO(pSId, 
							equivDTOList, PRListProcessor.aRSchedIdMap.get(pSId).get(schedDTO.getARId()).getEquivalentDTO(), 
							PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getMode(), 
							PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getPitchExtraBIC());
					
					// 2.1. Plan Equivalent DTO
					if (rulesPerformer.planEquivDTO(pSId, equivDTO, false)) {
																			
						unrankSchedDTOList.addAll(equivDTOList);								
				
						noStdDTOList.addAll(equivDTOList);		
					
					} else {
						
						logger.debug("Rejected Equivalent DTO in optimal scheduling: " 
								+ PRListProcessor.aRSchedIdMap.get(pSId)
								.get(schedDTO.getARId()).getEquivalentDTO().getEquivalentDtoId());
						
						planned = false;
					}
					
					for (DTO brmDTO : equivDTO.getAllDtoInEquivalentDto()) {
						
						noStdDTOIdList.add(brmDTO.getDtoId());
					}								
				}
				
			} else {
				
				// 2.4. Plan Equivalent DTO
				if (rulesPerformer.planSchedDTO(pSId, schedDTO)) {
					
					unrankSchedDTOList.add(schedDTO);
				
				} else {
					
					logger.debug("Rejected DTO in optimal scheduling: " + schedDTO.getDTOId());
					
					planned = false;
				}
			}
				
			if (i == schedSol.size() - 1) {
				
				logger.info("The complete set of unranked AR List has been processed.");

			}
		}
			
		return planned;
	}
	/**
	 * Get the owner Id related to the given scheduling AR Id
	 * 
	 * @param pSId
	 * @param schedARId
	 * @return
	 * @throws Exception
	 */
	private static ArrayList<String> getSchedAROwnerIdList(Long pSId, String schedARId) throws Exception {

		/**
		 * The output AR owner list
		 */
		ArrayList<String> aROwnerList = new ArrayList<>();

		if (!SessionActivator.ownerListMap.get(pSId).isEmpty()) {

			for (int i = 0; i < SessionActivator.ownerListMap.get(pSId).size(); i++) {

				if (!SessionActivator.ownerARIdMap.get(pSId).isEmpty()) {

					if (SessionActivator.ownerARIdMap.get(pSId).containsKey(
							SessionActivator.ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId())) {

						// logger.debug("Associate AR Ids for owner: "
						// +
						// ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId());

						if (SessionActivator.ownerARIdMap.get(pSId).get(SessionActivator.ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId())
								.contains(schedARId)) {
							
							// Add AR Owner
							aROwnerList.add(SessionActivator.ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId());
						}
					}

				} else {

					logger.warn("The list of AR Ids is empty.");
				}
			}

		} else {

			logger.warn("The list of owners is empty.");
		}

		return aROwnerList;
	}


}
