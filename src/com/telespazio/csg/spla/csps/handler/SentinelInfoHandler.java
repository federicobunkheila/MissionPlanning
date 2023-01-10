/**
 *
 * MODULE FILE NAME:	ConflictReportMessage.java
 *
 * MODULE TYPE:		<Class definition>
 *
 * FUNCTION:			<Functional description of the DDC>
 *
 * PURPOSE:          Receive, Process and Send Messages 
 *
 * CREATION DATE:	22-JAN-2017
 *
 * AUTHORS:			Tommaso Grenga
 *
 * DESIGN ISSUE:		1.0
 *
 * INTERFACES:		<prototype and list of input/output parameters>
 *
 * SUBORDINATES:		<list of functions called by this DDC>
 *
 * MODIFICATION HISTORY:
 *
 *             Date          |  Name      |   New ver.     | Description
 * --------------------------+------------+----------------+-------------------------------
 * <22-JAN-2017>             | Tommaso Grenga    |1.0     | first release
 * --------------------------+------------+----------------+-------------------------------
 *
 * PROCESSING
 */

package com.telespazio.csg.spla.csps.handler;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.splaif.protobuf.ConflictReportMessage.ConflictReport;
import com.telespazio.splaif.protobuf.SentinelInfoMessage.SentinelInfo;
import com.telespazio.splaif.protobuf.SentinelInfoMessage.SentinelInfo.Builder;

import it.sistematica.spla.datamodel.core.enums.PRKind;
import it.sistematica.spla.datamodel.core.enums.PRStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;

/**
 * Handler for manage ConflictReport Message
 */
public class SentinelInfoHandler
{
	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(FilterDTOHandler.class);

	/**
	 * Default Constructor
	 */
	public SentinelInfoHandler()
	{
		// Default Constructor
	}


	/**
	 * Get the Sentinel Info
	 * 
	 * @param sessionState
	 *            message for identifier S-PLA session state
	 * @param conflictReport
	 *            the session conflict report
	 * @param pSStatus 
	 * 		     the Planning Session status        
	 * @param hSId 
	 * 		     the SCM Check boolean
	 * @return 
	 * @return
	 * @throws Exception 
	 */
	public Builder getSentinelInfo(Long pSId) throws Exception
	{

		logger.debug("Create Sentinel Info Message for Planning Session: " + pSId);

		/**
		 *  Instance handlers				
		 */
		PRListProcessor pRListProcessor = new PRListProcessor();

		PersistPerformer persistPerformer = new PersistPerformer();
		
		// Method for create SentinelInfo
		SentinelInfo.Builder message = SentinelInfo.newBuilder();

		// Set Activate message
		message.setActivate(SessionActivator.activateMap.get(pSId));

		//			sessionScheduler.finalizeSchedule(pSId);			

		/**
		 * The list of Plan Request Status
		 */
		List<PlanProgrammingRequestStatus> planPRStatuses = SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList();

		logger.debug("Planning PR Statuses sent for Planning Session: " + pSId + ": "); 
		logger.debug(planPRStatuses.toString());

		/**
		 * The synchronous conflict report builder
		 */
		ConflictReport.Builder builder = ConflictReport.newBuilder().clear()
				.setPlanningSessionId(pSId).setReport(MessageHandler.serializeByteString(planPRStatuses))
				.setSubscribingReport(MessageHandler.serializeByteString(pRListProcessor.getPlanSessionSubPRStatuses(pSId)))
				.setSavedProgrammingRequestList(MessageHandler.serializeByteString(persistPerformer.getAllProgrammingRequest(pSId)))
				.setResult(true).setPlanningSentinels(getInputSentinelIds(pSId).size()).setPlannedSentinels(getOutputSentinelIds(pSId).size());

		message.setConflictReport(builder);

		message.setSessionSentinelInfo(SessionActivator.scmAvailMap.get(pSId));
		
		return message;

	}


	/**
	 * Get input SENTINEL PRs
	 * @param pSId
	 * @return
	 */
	public ArrayList<String> getInputSentinelIds(Long pSId) {

		/**
		 * The input PR SENTINEL Ids
		 */
		ArrayList<String> inSchedPRIdList = new ArrayList<String>();		

		for (ProgrammingRequest pR : PRListProcessor.pRListMap.get(pSId)) {

			if (pR.getKind().equals(PRKind.SENTINEL)) {

				logger.info("Planning input PR SENTINEL: " + pR.getProgrammingRequestId() 
				+ " for UGS: " + pR.getUgsId());

				inSchedPRIdList.add(ObjectMapper.parseDMToSchedPRId(
						pR.getUgsId(), pR.getProgrammingRequestId()));

			}
		}

		return inSchedPRIdList;
	}

	/**
	 * Get output PR SENTINELs
	 * @param pSId
	 * @return
	 */
	public ArrayList<String> getOutputSentinelIds(Long pSId) {

		/**
		 * The output count of the sentinels
		 */
		ArrayList<String> outSchedPRIdList = new ArrayList<String>();

		for (PlanProgrammingRequestStatus pRStatus : SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()) {

			for (ProgrammingRequest pR : PRListProcessor.pRListMap.get(pSId)) {

				// The scheduling PRId
				String schedPRId = ObjectMapper.parseDMToSchedPRId(
						pR.getUgsId(), pR.getProgrammingRequestId());
				
				if (schedPRId.equals(ObjectMapper.parseDMToSchedPRId(pRStatus.getUgsId(), 
						pRStatus.getProgrammingRequestId()))) {

					if (pRStatus.getStatus().equals(PRStatus.Scheduled)
							&& pR.getKind().equals(PRKind.SENTINEL)
							&& !outSchedPRIdList.contains(schedPRId)) {

						logger.info("Planned output PR SENTINEL: " + pR.getProgrammingRequestId() 
						+ " for UGS: " + pR.getUgsId());

						outSchedPRIdList.add(schedPRId);
					}
				}
			}
		}

		return outSchedPRIdList;
	}	
}
