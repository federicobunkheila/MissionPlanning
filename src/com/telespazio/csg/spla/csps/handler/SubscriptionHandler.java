/**
*
* MODULE FILE NAME: SubscriptionHandler.java
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

package com.telespazio.csg.spla.csps.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.telespazio.csg.spla.csps.core.server.Configuration;

import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.utils.AoICalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SchedARIdRankComparator;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.Category;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.SubscribingRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanSubscribableRequest;
import it.sistematica.spla.datamodel.core.model.PlanSubscribingRequestStatus;
import it.sistematica.spla.datamodel.core.model.resource.Owner;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogUgs;

/**
 * The subscription handler class.
 */
public class SubscriptionHandler {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(SubscriptionHandler.class);

	/**
	 * The list of already planned DTO Ids for ugs subscriber
	 */
	private static HashMap<String, ArrayList<String>> subUgsIdPlanDTOIdList = new HashMap<String, ArrayList<String>>();


	/**
	 * Collect the Subscription PRs statuses
	 *
	 * @param pSId
	 */
	public void collectSubscriptionStatuses(Long pSId) throws Exception {

		try {
			
			// 1.0. Detect subscriptability
			logger.info("Detect requests subscriptability...");
			detectSubscriptability(pSId);
	
			// 2.0. Verify subscription
			logger.info("Verify requests subscription...");
			verifySubscription(pSId);
		
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}

	/**
	 * Detect PRList subscriptability
	 *
	 * @param pSId
	 */
	private void detectSubscriptability(Long pSId) throws Exception {

		/**
		 * Instance handlers
		 */
		AoICalculator aoICalculator = new AoICalculator();

		logger.info("Find subscribable DTOs...");

		/**
		 * The subscription detection counter
		 */
		int detSubCount = 0;

		// 1.0 Find subscribable DTOs
		for (String rejDTOId : SessionScheduler.rejARDTOIdSetMap.get(pSId)) {

			logger.debug("Search subscriptability for rejected DTO: " + rejDTOId);
			
			if (!subUgsIdPlanDTOIdList.containsKey(ObjectMapper.getUgsId(rejDTOId))) {
				subUgsIdPlanDTOIdList.put(ObjectMapper.getUgsId(rejDTOId), new ArrayList<>());
			}

			/**
			 * The plan subscribable request
			 */
			PlanSubscribableRequest planSubRequest = null;

			for (String planDTOId : SessionScheduler.planDTOIdListMap.get(pSId)) {
				
				// Added on 23/12/2021 for matching catalog suscribers compatibility
				
				// 1.1 Added on 25/08/2021 for matching ugs subscription compatibility				
				if (subUgsIdPlanDTOIdList.containsKey(ObjectMapper.getUgsId(rejDTOId))
					&& ! subUgsIdPlanDTOIdList.get(ObjectMapper.getUgsId(rejDTOId)).contains(planDTOId)) {
			
					/**
					 * Planned ugsId
					 */
					String planUgsId = ObjectMapper.getUgsId(planDTOId);
				
					/**
					 * Rejected ugsId
					 */
					String rejUgsId = ObjectMapper.getUgsId(rejDTOId);
					
					/**
					 * Rejected ownerId
					 */
					String rejOwnerId = SessionActivator.ugsOwnerIdMap.get(pSId)
							.get(rejUgsId);
					/**
					 * Rejected owner
					 */
					Owner rejOwner = null;
						
					// Find rejected  owner
					for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {
						
						if (owner.getCatalogOwner().getOwnerId().equals(rejOwnerId))  {
							
							rejOwner = owner;
						}
					}
				/**
				 * Subscription availability
				 */
				boolean subAvail = false;
				
				if (ObjectMapper.getSchedARId(planDTOId).equals(
						ObjectMapper.getSchedARId(rejDTOId))) {

					continue;
				}
								
				// 1.2. Check subscription compatibility
				for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {
					
					// 1.3. Check self owner compatibility
					if (planUgsId.equals(rejUgsId)
						&& !isSelfSubCompatible(pSId, rejUgsId)) {
						
						subAvail = false;
						
						break;
					}
					// 1.4 Check subscription availability
					if (owner.getCatalogOwner().getOwnerId().equals(rejOwnerId)) {
					
						for  (CatalogUgs subUgsCat : rejOwner.getCatalogOwner().getUgsList()) {
						
							for (String subUgsId : subUgsCat.getUgsSubscriptionCompatibility()) {
								
								if (subUgsId != null && planUgsId != null 
										&& subUgsId.equals(planUgsId)) {
									
									subAvail = true;
									
									break;
								}
							}
							
							if (subAvail) {
							
								break;
							}						
						}
						
						if (subAvail) {
							
							break;
						}
					}
				}
				
				if (subAvail && !ObjectMapper.getSchedARId(planDTOId)
						.equals(ObjectMapper.getSchedARId(rejDTOId))) {

					// 2.0 Detect subscription
					
					logger.trace("Detect subscription with planned DTO: " + planDTOId);


					if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(ObjectMapper.getSchedPRId(rejDTOId))
							&& PRListProcessor.pRSchedIdMap.get(pSId).containsKey(
									ObjectMapper.getSchedPRId(planDTOId))) {

						logger.trace("PRs Ids found.");

						// Handle confidential level
						if ((PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(rejDTOId))
								.getConfidentialLevel() == null)
								|| (PRListProcessor.pRSchedIdMap.get(pSId)
										.get(ObjectMapper.getSchedPRId(planDTOId))
										.getConfidentialLevel() == null)) {

							if (PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(rejDTOId))
									.getCategory().equals(Category.Defence)) {
							
								logger.debug("No confidential level of PRs found.");
							}

							PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(rejDTOId))
									.setConfidentialLevel("");

							PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
									.setConfidentialLevel("");
						}

						// 2.1 Check subscription conditions
						if (PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(rejDTOId))
								.getAvailableForSubscription()
								&& PRListProcessor.pRSchedIdMap.get(pSId)
										.get(ObjectMapper.getSchedPRId(planDTOId)).getAvailableForSubscription()
								&& PRListProcessor.pRSchedIdMap.get(pSId)
										.get(ObjectMapper.getSchedPRId(rejDTOId)).getConfidentialLevel()
										.equals(PRListProcessor.pRSchedIdMap.get(pSId)
												.get(ObjectMapper.getSchedPRId(planDTOId))
												.getConfidentialLevel())) {

							// Check DTO existence
							if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(rejDTOId)
									&& PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(planDTOId)) {

								logger.trace("DTOs Ids found.");

								// 2.2. Search sensor modes compatibility				   
								if (PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getSensorMode().equals(
										PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getSensorMode())) {

									if ((PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId)
											.getAreaOfInterest() != null)
											&& !PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId)
													.getAreaOfInterest().isEmpty()) {

										logger.trace("Search AoI compatibility.");

										// 2.3. Search AoI compatibility
										if (aoICalculator.isIntersectedAoI(
												PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId)
														.getAreaOfInterest(),
												PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId)
														.getAreaOfInterest())) {

											logger.trace("Search PR Mode compatibility.");

											// 2.4. Search PR modes compatibility
											if (PRListProcessor.pRSchedIdMap.get(pSId)
													.get(ObjectMapper.getSchedPRId(rejDTOId)).getMode()
													.equals(PRMode.Standard)
													&& PRListProcessor.pRSchedIdMap.get(pSId)
															.get(ObjectMapper.getSchedPRId(planDTOId)).getMode()
															.equals(PRMode.Standard)) {

												// 2.5. Set subscription statuses
												planSubRequest = setSubscriptionStatuses(pSId, planDTOId, rejDTOId);

												// 2.6. Add planned DTO Id to the subscriber ugs map
												// with respect to ugs applicability
												subUgsIdPlanDTOIdList.get(ObjectMapper.getUgsId(rejDTOId)).add(planDTOId);
		

												if (planSubRequest != null) {

													break;
												}
											}
										}

									} else {

										logger.warn("Area of Interest not parsable for DTO: " + rejDTOId
												+ ". The request is not subscribable.");
									}
								}
							}
						}
					}
				}
			}
			}

			if (planSubRequest != null) {

				// 3.0. Add subscribable request
				for (int i = 0; i < SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()
						.size(); i++) {

					if (ObjectMapper.getSchedPRId(rejDTOId).contains(SessionActivator.planSessionMap.get(pSId)
							.getProgrammingRequestStatusList().get(i).getProgrammingRequestId())) {

						for (int j = 0; j < SessionActivator.planSessionMap.get(pSId)
								.getProgrammingRequestStatusList().get(i).getAcquisitionRequestStatusList()
								.size(); j++) {

							if (ObjectMapper.getSchedARId(rejDTOId)
									.contains(SessionActivator.planSessionMap.get(pSId)
											.getProgrammingRequestStatusList().get(i)
											.getAcquisitionRequestStatusList().get(j).getAcquisitionRequestId())) {

								// 3.1 Add subscribable PR			
								SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().get(i)
										.getAcquisitionRequestStatusList().get(j).addtSubscribable(planSubRequest);

								detSubCount++;
								logger.debug("Detected planning subscribable request:  " + planSubRequest);

								break;
							}
						}
					}
				}
			}
		}

		logger.info("A number of " + detSubCount + " DTO requests is detected as "
				+ "subscribable for Planning Session: " + pSId);
			
		// Clear temp map  
		subUgsIdPlanDTOIdList.clear();

	}

	/**
	 * Verify the scheduling of subscribed request
	 *
	 * @param pSId
	 */
	private void verifySubscription(Long pSId) throws Exception {

		/**
		 * The subscription counter
		 */	
		int subCount = 0;

		logger.debug("Scheduling of a number of "
				+ SessionActivator.planSessionMap.get(pSId).getSubscribingRequestStatusList().size()
				+ " subscribed DTO requests has to be verified for Planning Session: " + pSId);

		for (int i = 0; i < SessionActivator.planSessionMap.get(pSId).getSubscribingRequestStatusList()
				.size(); i++) {

			/**
			 * The status of the subscribing PR
			 */
			PlanSubscribingRequestStatus subPRStatus = SessionActivator.planSessionMap.get(pSId)
					.getSubscribingRequestStatusList().get(i);

			logger.debug("Verify the scheduling of subscribing request with: PR Id "
					+ subPRStatus.getProgrammingRequestId() + ", AR Id " + subPRStatus.getAcquisitionRequestId()
					+ ", DTO Id " + subPRStatus.getDtoId());

			// Set request statuses
			for (PlanProgrammingRequestStatus pRStatus : SessionActivator.planSessionMap.get(pSId)
					.getProgrammingRequestStatusList()) {

				for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {

					if (aRStatus.getStatus().equals(AcquisitionRequestStatus.Scheduled)) {

						for (PlanDtoStatus dtoStatus : aRStatus.getDtoStatusList()) {

							if (dtoStatus.getStatus().equals(DtoStatus.Scheduled)) {

								if (subPRStatus.getUgsId().equals(pRStatus.getUgsId())
										&& subPRStatus.getProgrammingRequestListId()
												.equals(pRStatus.getProgrammingRequestListId())
										&& subPRStatus.getProgrammingRequestId()
												.equals(pRStatus.getProgrammingRequestId())
										&& subPRStatus.getAcquisitionRequestId()
												.equals(aRStatus.getAcquisitionRequestId())) {

									// Set the scheduled Plan Subscribing Request status
									subPRStatus.setStatus(SubscribingRequestStatus.ScheduledBySubscription);

									logger.debug("The scheduling of the subscribed request is verified.");

									SessionActivator.planSessionMap.get(pSId).getSubscribingRequestStatusList()
											.set(i, subPRStatus);

									subCount++;									

									break;
								}
							} 
							else if (dtoStatus.getStatus().equals(DtoStatus.Rejected))
							{
								
								// Set the description NotScheduled Plan Subscribing Request status  
								// subPRStatus.setDescription(); 
								// TODO: set description according to DataModel
							}
						}
					}
				}			
			}
		}

		logger.info("A number of " + subCount + " subscribed DTO requests is verified "
				+ "as scheduled for Planning Session: " + pSId);
	}

	/**
	 * Set Subscription Statuses
	 * 
	 * @param pSId
	 * @param planDTOId
	 * @param rejDTOId
	 */
	private PlanSubscribableRequest setSubscriptionStatuses(Long pSId, String planDTOId, String rejDTOId) {

		logger.debug("Search NEO compatibility.");

		/**
		 * The Plan Subscribable Request
		 */
		PlanSubscribableRequest planSubRequest = null;

		if ((!RequestChecker.isNEO((PRListProcessor.pRSchedIdMap.get(pSId)
				.get(ObjectMapper.getSchedPRId(rejDTOId)).getVisibility()))
				&& !RequestChecker.isNotNEO((PRListProcessor.pRSchedIdMap.get(pSId)
					.get(ObjectMapper.getSchedPRId(rejDTOId)).getVisibility())))
					&& (!RequestChecker.isNEO((PRListProcessor.pRSchedIdMap.get(pSId)
						.get(ObjectMapper.getSchedPRId(planDTOId)).getVisibility()))
						&& !RequestChecker.isNotNEO((PRListProcessor.pRSchedIdMap.get(pSId)
							.get(ObjectMapper.getSchedPRId(planDTOId)).getVisibility())))) { // case !NEO && !NotNeo

			logger.debug("NEO compatibility is found.");

			// subCount++;

			// Instance Plan Subscribable Request
			planSubRequest = new PlanSubscribableRequest(
					PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
						.getUserList().get(0).getUgsId(),
					PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
						.getProgrammingRequestListId(),
					PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
						.getProgrammingRequestId(),
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(planDTOId))
						.getAcquisititionRequestId(),
					PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getDtoId(), 
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(planDTOId))
						.getUniqueId());

			logger.debug("Detected subscribable request with:" + " PR Id " + planSubRequest.getProgrammingRequestId()
					+ ", AR Id " + planSubRequest.getAcquisitionRequestId() + ", DTO Id " + planSubRequest.getDtoId()
					+ " for DTO: " + rejDTOId);
			
		} else if (RequestChecker.isNEO(PRListProcessor.pRSchedIdMap.get(pSId)
					.get(ObjectMapper.getSchedPRId((rejDTOId))).getVisibility())
					&& RequestChecker.isNEO(PRListProcessor.pRSchedIdMap.get(pSId)
						.get(ObjectMapper.getSchedPRId(planDTOId)).getVisibility())) { // case NEO

			// Find Plan Subscribable Request
			if (PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(rejDTOId))
					.getUserList().get(0).getUgsId().equals(PRListProcessor.pRSchedIdMap.get(pSId)
						.get(ObjectMapper.getSchedPRId(planDTOId)).getUserList().get(0).getUgsId())) {

				// subCount++;

				// Set Plan Subscribable Request
				planSubRequest = new PlanSubscribableRequest(
						PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
								.getUserList().get(0).getUgsId(),
						PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
								.getProgrammingRequestListId(),
						PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
								.getProgrammingRequestId(),
						PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(planDTOId))
								.getAcquisititionRequestId(),
						PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getDtoId(), PRListProcessor.aRSchedIdMap
								.get(pSId).get(ObjectMapper.getSchedARId(planDTOId)).getAcquisititionRequestId());

				logger.debug("Detected subscribable request with:" + " PR Id " + planSubRequest.getProgrammingRequestId()
								+ ", AR Id " + planSubRequest.getAcquisitionRequestId() + ", DTO Id "
								+ planSubRequest.getDtoId() + " for DTO: " + rejDTOId);
			}

		} else if (RequestChecker.isNotNEO(PRListProcessor.pRSchedIdMap.get(pSId)
					.get(ObjectMapper.getSchedPRId((rejDTOId))).getVisibility())
					&& RequestChecker.isNotNEO(PRListProcessor.pRSchedIdMap.get(pSId)
							.get(ObjectMapper.getSchedPRId(planDTOId)).getVisibility())) { // case NotNEO

			// subCount++;

			// Set Plan Subscribable Request
			planSubRequest = new PlanSubscribableRequest(
					PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId)).getUserList()
							.get(0).getUgsId(),
					PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
							.getProgrammingRequestListId(),
					PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId))
							.getProgrammingRequestId(),
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(planDTOId))
							.getAcquisititionRequestId(),
					PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getDtoId(), PRListProcessor.aRSchedIdMap
							.get(pSId).get(ObjectMapper.getSchedARId(planDTOId)).getAcquisititionRequestId());

			logger.debug("Detected subscribable request with:" + " PR Id " + planSubRequest.getProgrammingRequestId()
					+ ", AR Id " + planSubRequest.getAcquisitionRequestId() + ", DTO Id " + planSubRequest.getDtoId()
					+ " for DTO: " + rejDTOId);

		}

		return planSubRequest;
	}
	
	/**
	 * Check Self-Subscription compatibility for ugs
	 * @param pSId
	 * @param ugsId
	 * @return
	 */
	private boolean isSelfSubCompatible(Long pSId, String ugsId) {
	
		/**
		 * The self compatibility boolean
		 */
		boolean isSelfCompatible = true;
		
		if (Configuration.noSelfSubCompatibleUgsList.contains(ugsId)) {
			
			isSelfCompatible = false;
		}
		
		return isSelfCompatible;
	}
}
