/**
*
* MODULE FILE NAME: DeltaPlanProcessor.java
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.ontology.enums.ReasonOfReject;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.splaif.protobuf.FilteringMessage.FilteringResult.RejectedRequest;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.SubscribingRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanSubscribingRequestStatus;																		 

/**
 * The Filtering DTO handler class
 */
public class FilterDTOHandler {

	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(FilterDTOHandler.class);

	/**
	 * The filtered request list map
	 */
	public static HashMap<Long, List<RejectedRequest>> filtRejReqListMap;

	/**
	 * The filtered request DTO Id list map
	 */
	public static HashMap<Long, ArrayList<String>> filtRejDTOIdListMap;

	/**
	 * The waiting filtering result map
	 */
	public static HashMap<Long, Boolean> isWaitFiltResultMap;

	/**
	 * Handle the rejected requests from SCM where both owners and subscribers are included
	 *
	 * @param pSId
	 * @param rejReqList
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	public void handleRejRequests(Long pSId, List<RejectedRequest> rejReqList) throws Exception {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();
		
		logger.info("Handle SCM rejections...");

		try {		

			if (rejReqList != null) {
			
				filtRejReqListMap.put(pSId, rejReqList);
		
				// Handle rejections for DTO owners and subscribers
				for (RejectedRequest rejReq : filtRejReqListMap.get(pSId)) {
		
					for (String rejUgsId : rejReq.getUgsIdList()) {
					
						filtRejDTOIdListMap.get(pSId).add(ObjectMapper.parseDMToSchedDTOId(rejUgsId,
								rejReq.getProgrammingRequestId(), rejReq.getAcquisitionRequestId(), rejReq.getDtoId()));
					}					
				}
	
			} else {
				
				logger.debug("Null rejection list found...");
			}
			
			/**
			 * The list of Plan PR statuses
			 */
			ArrayList<PlanProgrammingRequestStatus> pRStatusList = (ArrayList<PlanProgrammingRequestStatus>) 
					((ArrayList<PlanProgrammingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
					.getProgrammingRequestStatusList()).clone();
			
			for (int i = 0; i < pRStatusList.size(); i++) {
	
				/**
				 * The list of Plan AR statuses
				 */
				List<PlanAcquisitionRequestStatus> aRStatusList = SessionActivator.planSessionMap.get(pSId)
						.getProgrammingRequestStatusList().get(i).getAcquisitionRequestStatusList();
	
				for (int j = 0; j < aRStatusList.size(); j++) {
	
					/**
					 * The list of Plan DTO statuses
					 */
					List<PlanDtoStatus> dtoStatusList = aRStatusList.get(j).getDtoStatusList();
	
					/**
					 * The request availability
					 */
					boolean reqAvail = false;
	
					for (int k = 0; k < dtoStatusList.size(); k++) {
	
						// 1.1 Check SCM filtering for direct rejection
						for (RejectedRequest rejReq : FilterDTOHandler.filtRejReqListMap.get(pSId)) {
	
							if (rejReq.getUgsIdList().get(0).equals(pRStatusList.get(i).getUgsId())
									&& rejReq.getProgrammingRequestId().equals(pRStatusList.get(i).getProgrammingRequestId())
									&& rejReq.getAcquisitionRequestId().equals(aRStatusList.get(j).getAcquisitionRequestId())
									&& rejReq.getDtoId().equals(dtoStatusList.get(k).getDtoId())) {
	
								logger.info("The DTO " + rejReq.getDtoId() + " of AR " + rejReq.getAcquisitionRequestId() 
								+ " of PR " + rejReq.getProgrammingRequestId() + " for UGS " + rejReq.getUgsIdList().get(0) 
								+ " is rejected due to SCM filtering.");
	
								// Set conflict status // TODO: check with Ground!
								dtoStatusList.get(k).setStatus(DtoStatus.Rejected);
								dtoStatusList.get(k).setConflictDescription("System Conflict.");
								dtoStatusList.get(k).setConflictReasonId(1); // TODO: finalize conflict reason							
								
								/**
								 * The filtering DTO Id
								 */
								String filtDTOId = ObjectMapper.parseDMToSchedDTOId(
										rejReq.getUgsIdList().get(0), rejReq.getProgrammingRequestId(), 
										rejReq.getAcquisitionRequestId(), rejReq.getDtoId());
								
								// Delete DTO id if previously accepted
								if (RulesPerformer.getPlannedDTOIds(pSId).contains(filtDTOId)) {
									
									rulesPerformer.retractDTOById(pSId, filtDTOId, ReasonOfReject.systemConflict); // TODO: system Conflict
								}
								
								aRStatusList.get(j).getDtoStatusList().set(k, dtoStatusList.get(k));								
							}
						}
	
						if (! aRStatusList.get(j).getDtoStatusList().get(k).getStatus().equals(DtoStatus.Rejected)) {
	
							reqAvail = true;
						}
					}
	
					if (!reqAvail) {
	
						aRStatusList.get(j).setStatus(AcquisitionRequestStatus.Rejected);
					}
	
					pRStatusList.get(i).getAcquisitionRequestStatusList().set(j, aRStatusList.get(j));
				}
				
				SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().set(i, pRStatusList.get(i));
			}
			
			/**
			 * The list of Plan Subscribing PR statuses
			 */
			ArrayList<PlanSubscribingRequestStatus> subPRStatusList = (ArrayList<PlanSubscribingRequestStatus>) 
					((ArrayList<PlanSubscribingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
					.getSubscribingRequestStatusList()).clone();

			// 1.1 Check SCM filtering for subscription rejection
			
			for (int i = 0; i < subPRStatusList.size(); i++) {
	
	
				// 1.2 Check SCM filtering for subscribing rejection
				for (RejectedRequest rejReq : FilterDTOHandler.filtRejReqListMap.get(pSId)) {

					if (rejReq.getUgsIdList().get(0).equals(pRStatusList.get(i).getUgsId())
							&& rejReq.getProgrammingRequestId().equals(subPRStatusList.get(i).getProgrammingRequestId())
							&& rejReq.getAcquisitionRequestId().equals(subPRStatusList.get(i).getAcquisitionRequestId())
							&& rejReq.getDtoId().equals(subPRStatusList.get(i).getDtoId())) {

						logger.info("The subscribing DTO " + rejReq.getDtoId() + " of AR " + rejReq.getAcquisitionRequestId() 
						+ " of PR " + rejReq.getProgrammingRequestId() + " for UGS " + rejReq.getUgsIdList().get(0) 
						+ " is rejected due to SCM filtering.");

						// Set conflict status // TODO: check with Ground!
						subPRStatusList.get(i).setStatus(SubscribingRequestStatus.NotScheduledBySubscription);
						
						/**
						 * The filtering DTO Id
						 */
						String filtDTOId = ObjectMapper.parseDMToSchedDTOId(
								rejReq.getUgsIdList().get(0), rejReq.getProgrammingRequestId(), 
								rejReq.getAcquisitionRequestId(), rejReq.getDtoId());
						
						// Delete DTO id if previously accepted
						if (RulesPerformer.getPlannedDTOIds(pSId).contains(filtDTOId)) {
							
							rulesPerformer.retractSubDTOById(pSId, filtDTOId, ReasonOfReject.systemConflict); // TODO: system Conflict
						}
					}
				}
					
				SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().set(i, pRStatusList.get(i));
		}
		// Update waiting Filtering Result
		isWaitFiltResultMap.put(pSId, false);
		
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
 		
	}

	/**
	 * Filter previously rejected DTOs and possible shuttered requests
	 * 
	 * @param pSId
	 */
	public void handlePrevRejRequests(Long pSId) {
		
		/**
		 * Instance handlers
		 */
		FilterDTOHandler filterDTOHandler = new FilterDTOHandler();
		
		RulesPerformer rulesPerformer = new RulesPerformer();
				
		try {
			
			/**
			 * The list of rejected requests
			 */
			ArrayList<RejectedRequest> rejReqList = new ArrayList<RejectedRequest>();
			
			Iterator<Entry<Long, List<RejectedRequest>>> it = FilterDTOHandler.filtRejReqListMap.entrySet().iterator();
			
			while (it.hasNext()) {
			
				/**
				 * The list of rejected requests in the Mission Horizon
				 */
				List<RejectedRequest> mhRejReqList = it.next().getValue();
				
				for (RejectedRequest mhRejReq : mhRejReqList) {
					
					if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(ObjectMapper.parseDMToSchedPRId(
							mhRejReq.getUgsIdList().get(0), mhRejReq.getProgrammingRequestId()))) { 
				     
						rejReqList.add(mhRejReq);
					}
				}
			}
			
			// Handle rejected requests
			filterDTOHandler.handleRejRequests(pSId, rejReqList);
						
			/**
			 * The list of scheduled DTOs
			 */
			ArrayList<String> schedDTOIdList = RulesPerformer.getPlannedDTOIds(pSId);
			
			for (String schedDTOId : schedDTOIdList) {
				
				if (filtRejDTOIdListMap.get(pSId).contains(schedDTOId)) {
				
					rulesPerformer.retractDTOById(pSId, schedDTOId, ReasonOfReject.systemConflict);
				}

			}

		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}
	

}
