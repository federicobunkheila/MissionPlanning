/**
*
* MODULE FILE NAME: CSPSListener.java
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

package com.telespazio.csg.spla.csps.core.message.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.nais.spla.msl.main.core.MessageServiceLayer;
import com.nais.spla.msl.main.core.MessagesLabel;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.core.server.Environment;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.handler.MessageHandler;
import com.telespazio.csg.spla.csps.handler.SentinelInfoHandler;
import com.telespazio.csg.spla.csps.handler.SubscriptionHandler;
import com.telespazio.csg.spla.csps.model.impl.Partner;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.NextARProcessor;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.utils.BICCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.splaif.protobuf.ActivateMessage.Activate;
import com.telespazio.splaif.protobuf.ActivateMessage.ActivateAck;
import com.telespazio.splaif.protobuf.CloseSessionMessage.CloseSessionNotification;
import com.telespazio.splaif.protobuf.CloseSessionMessage.OperationResult;
import com.telespazio.splaif.protobuf.CloseSessionMessage.SetPlanningSessionStatus;
import com.telespazio.splaif.protobuf.CloseSessionMessage.UnrankedARSchedulabilityStatus;
import com.telespazio.splaif.protobuf.ConflictReportMessage.ConflictReport;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityResult;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityResult.DI2SProgrammingRequestSet;
import com.telespazio.splaif.protobuf.EnforceExclusionRuleMessage.EnforceExclusionRuleRequest;
import com.telespazio.splaif.protobuf.EnforceExclusionRuleMessage.EnforceExclusionRuleResult;
import com.telespazio.splaif.protobuf.FilteringMessage.FilteringResult;
import com.telespazio.splaif.protobuf.NextARMessage.NextAR;
import com.telespazio.splaif.protobuf.NextARMessage.NextARSchedulabilityStatus;
import com.telespazio.splaif.protobuf.PRListMessage.ManualPRList;
import com.telespazio.splaif.protobuf.PRListMessage.PRList;
import com.telespazio.splaif.protobuf.PRListMessage.PRListAck;
import com.telespazio.splaif.protobuf.SentinelInfoMessage;
import com.telespazio.splaif.protobuf.SentinelInfoMessage.SentinelInfo.Builder;
import com.telespazio.splaif.protobuf.StatusCheckmessage.SCMCheckResult;

import it.sistematica.spla.datamodel.core.enums.PlanningSessionType;
import it.sistematica.spla.datamodel.core.exception.InputException;
import it.sistematica.spla.datamodel.core.exception.SPLAException;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanSubscribingRequestStatus;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;

/**
 * The CSPS Listener class.
 */
public class CSPSListener implements MessageListener {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(CSPSListener.class);

	/**
	 * The javax session
	 */
	private Session session;

	/**
	 * The message service layer
	 */
	private MessageServiceLayer msl;

	/**
	 * The message handler
	 */
	private MessageHandler messageHandler;

	/**
	 * The message to be handled
	 */
	private Message message;

	/**
	 * The CSPS Listener
	 *
	 * @param session
	 * @param msl
	 */
	public CSPSListener(Session session, MessageServiceLayer msl) {

		// instance msl
		this.msl = msl;
		
		// Instance session
		this.session = session;

	}

	/**
	 * The on message service.
	 */
	@Override
	public void onMessage(Message msg) {

		try {

			// Instance elements
			this.messageHandler = new MessageHandler();

			if ((msg != null) && (msg instanceof BytesMessage) && (((BytesMessage) msg).getBodyLength() > 0)) {

				String messageType = msg.getStringProperty("messageType");

				logger.info("");
				logger.info("Message received. Type : " + messageType + " - Id : " + msg.getJMSMessageID());
				logger.info("Received message: " + msg);

				// Ack message in order to put out from the queue
				msg.acknowledge();

				this.message = msg;

				if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY).contains("Activate")) {

					// Handle the Planning Session activation
					handleActivate();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY).equalsIgnoreCase("PRList")) {

					// Handle the incoming PRList
					handlePRList();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY).contains("ManualPRList")) {

					// Handle the incoming Manual PRList
					handleManualPRList();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY).contains("NextAR")) {

					// Handle the incoming Next AR
					handleNextAR();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY)
						.contains("CloseSessionNotification")) {

					// Handle the notification of Planning Session closure
					handleCloseSessionNotification();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY)
						.contains("SetPlanningSessionStatus")) {

					// Handle the setting of the Planning Session status
					handleSetPlanningSessionStatus();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY)
						.contains("DI2SCompatibilityResult")) {

					// Handle the setting of the DI2S result
					handleDI2SResult();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY).contains("FilteringResult")) {

					// Handle the filtering result
					handleFilteringResult();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY)
						.contains("EnforceExclusionRuleRequest")) {

					// Handle the Enforce Exclusion Rule Request
					handleEnforceExclusionRuleRequest();

				} else if (msg.getStringProperty(MessagesLabel.MESSAGE_PROPERTY_KEY).contains("SCMCheckResult")) {

					// Handle SCM Check Result
					handleSCMCheckResult();

				} else {

					logger.warn("Not supported message class from CSPS!");
				}

			} else {

				logger.warn("Null or empty message raised!");
			}

			logger.debug("Clear Message sent: " + msg);

			msg.clearBody();

			msg.clearProperties();

		} catch (Exception ex) {

			logger.error("Error managing incoming message from MSL: " + ex.getStackTrace()[0].toString());
		}
	}

	/**
	 * Handle Activate message from BPM
	 *
	 * @throws JMSException
	 * @throws InvalidProtocolBufferException
	 * @throws SPLAException
	 */
	private synchronized void handleActivate() throws InvalidProtocolBufferException, 
	JMSException, SPLAException {

		/**
		 * Instance handlers
		 */
		SessionActivator sessionActivator = new SessionActivator();
	
		CSPSSender cspsSender = new CSPSSender(this.session, this.msl);

		// Parse message
		Activate activate = Activate.parseFrom(this.messageHandler.getMessageAsBytes(this.message));

		// The Planning Session Id
		Long pSId = activate.getPlanningSessionId();

		// The output result
		boolean result = false;
		
		try {

			logger.debug("Received " + this.message.getStringProperty("messageType") + " message from BPM "
					+ activate.toString());

			logger.debug("Session Id: {} - MH Start Time: {} - MH Stop Time: {}", pSId,
					activate.getMissionHorizonStartTime(), activate.getMissionHorizonStopTime());

			logger.info("***** Start of the CSPS scheduling session: " + pSId);

			// Activate the planning session
			result = sessionActivator.activate(activate);
			
			// Respond Activate Ack
			respondActivateAck(pSId, result);

			// Check Veto enabling
			if (SessionChecker.isVetoEnabled(pSId)) {

				// SendSCM check
				cspsSender.sendSCMCheck(pSId);

//				SessionActivator.scmAvailMap.put(pSId, false);
			}

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
			
			respondActivateAck(pSId, false);
			
		}
	}
	
		/**
	 * Respond Activate acknowledge
	 * @param pSId
	 * @param result
	 * @throws JMSException 
	 */
	private synchronized void respondActivateAck(Long pSId, boolean result) throws JMSException {


			// Send back Activate Ack
			ActivateAck.Builder builder = ActivateAck.newBuilder().clear()
					.setPlanningSessionId(pSId).setAccepted(result);

			logger.info("Sending " + ActivateAck.class.getSimpleName() + " message to BPM.");
			logger.debug("Message sent: " + builder.toString());

			// Send Broadcast message
			this.msl.sendBrodcastMessage(createBytesMessage(builder.build()),
					Environment.getConfiguration().getModuleName(), ActivateAck.class.getSimpleName());

			logger.info(ActivateAck.class.getSimpleName() + " message sent.");
	}

	/**
	 * Handle PRList message from DHM
	 * // TODO: check VETO for Asynchronous Planning Sessions
	 * @throws JMSException
	 * @throws InvalidProtocolBufferException
	 */
	private synchronized void handlePRList() throws InvalidProtocolBufferException, JMSException {

		/**
		 * Instance handlers
		 */
		PRListProcessor pRListProcessor = new PRListProcessor();
		
		SessionScheduler sessionScheduler = new SessionScheduler();

		try {

			// Parse message
			PRList pRList = PRList.parseFrom(this.messageHandler.getMessageAsBytes(
					this.message));

			/**
			 * The Planning Session Id
			 */
			Long pSId = pRList.getPlanningSessionId();
			
			logger.debug("Received " + this.message.getStringProperty("messageType") + " message from DHM: "
					+ pRList.toString());

			logger.debug("Session Id: {} - Num Elem: {} - isAsynchronous: {}", pSId, pRList.getProgReqListCount(),
					pRList.getIsAsynchronous());
			
			/**
			 * The result boolean
			 */
			boolean result = false;
			
			// Differentiate between Nominal and VU or LMP scenarios
			if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
					.equals(PlanningSessionType.VeryUrgent)) {

				System.out.println("");
				logger.info("Process the incoming Very Urgent PRList...");

				// Import VU PRList
				result = pRListProcessor.importVUPRList(pRList);

			} else if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
					.equals(PlanningSessionType.LastMinutePlanning)) {
				
				System.out.println("");	
				logger.info("Process the incoming Last Minute Planning PRList...");
				
				// Import LMP PRList
				result = pRListProcessor.importLMPPRList(pRList);

			} else {

				logger.info("Process the incoming PRList...");

				// Import Nominal PRList
				result = pRListProcessor.importPRList(pRList);
			}
			
			// Check Veto enabling
			if (SessionChecker.isVetoEnabled(pSId)) {

				/**
				 * The CSPS Sender
				 */
				CSPSSender cspsSender = new CSPSSender(this.session, this.msl);

				// Send Filtering Request
				cspsSender.sendFilteringRequest(pSId, pRListProcessor.getPlanSessionPRList(pSId), 
						SessionActivator.planDateMap.get(pSId).toString());
				
				// Update waiting Filtering Map
				FilterDTOHandler.isWaitFiltResultMap.put(pSId, true);
				
				// The SCM waiting dates	
				Date date = new Date();
				
				Date date0 = new Date(); 

				logger.debug("Waiting for SCM Veto filtering response for Planning Session: " + pSId);

				// Wait SCM response
				while ((date.getTime() - date0.getTime() < Configuration.sendTimeout) 
						&& SessionActivator.scmResWaitMap.get(pSId)) {
										
					date = new Date();
				}
				
			} else {

				logger.info("No SCM Veto filtering is required for Planning Session: " + pSId);
			}

			if (SessionChecker.isUnranked(pSId)) { // Case Unranked
					
				// Respond the Unranked AR schedulability status
				respondUnrankedStatus(pSId);
				
			} else if (SessionChecker.isFinal(pSId) && ! SessionChecker.isSelf(pSId)) { // Case VU/LMP

				// Respond the Asynchronous Conflict Report
				respondConflictReport(pSId, true, result);				
			
			} else if (! SessionChecker.isSelf(pSId)) { // Case HP/PP/RR
			
				// Respond the PRList Acknowledge
				respondPRListAck(pSId, result);
			
			} else {
				
				// Case of Self-Generated, NOT handled in @handleFilteringResult								
				if (! Configuration.waitSCMFlag) {
	 
					logger.info("SCM Waiting flag is false, Planning Session is going to be saved.");		
				}
				
				PersistPerformer persistPerformer = new PersistPerformer();
				
				// Save session schedule info
				logger.info("Persist Session Info for Planning Session: " + pSId);
				boolean ack1 = persistPerformer.persistSession(pSId);

				// Save resource values
				logger.info("Persist Resources for Planning Session: " + pSId);
				boolean ack2 = persistPerformer.persistResources(pSId);

				if (ack1 && ack2) {

					logger.info("Session Info and Resource Values for Planning Session: " + pSId + " saved.");

				} else {

					logger.warn("Session Info and Resource Values for Planning Session: " + pSId + " NOT saved.");

					result = false;
				}
				
				// Write BRM report file
				RulesPerformer.writeBRMReportFile(pSId);
				
				if (! SessionScheduler.persistenceMap.containsKey(pSId)) {	
					
					SessionScheduler.persistenceMap.put(pSId, "true");									  
				}
				
				// Respond Operation Result
				respondOperationResult(pSId, result);
							
				// Added on 22/1/2021 for Sentinel Info message handler
				sendSentinelInfo(pSId,  result);
				
				// Close session elements
				sessionScheduler.closeSession(pSId);
			}

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
		}
	}

    /**
	 * Handle Manual PRList message from DHM 
	 * // TODO: filtering SCM // TODO: planning unranked ARs
	 * 
	 * @throws JMSException
	 * @throws InvalidProtocolBufferException
	 */
	private synchronized void handleManualPRList() {

		/**
		 * Instance handlers
		 */
		PRListProcessor pRListProcessor = new PRListProcessor();

		PersistPerformer persistPerformer = new PersistPerformer();
		
		SessionScheduler sessionScheduler = new SessionScheduler();

		/**
		 * Output boolean
		 */
		boolean result = true;

		try {
			
			// Parse message
			ManualPRList manPRList = ManualPRList.parseFrom(
					this.messageHandler.getMessageAsBytes(this.message));

			/**
			 * The Planning Session Id
			 */
			Long pSId = manPRList.getPlanningSessionId();
			
			/**
			 * The Planning Session Id
			 */
			logger.debug("Received " + this.message.getStringProperty("messageType") 
				+ " message from BPM " + manPRList.toString() +  " for Planning Session: " + pSId);
			
			// Check manual PRList
			if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
					.equals(PlanningSessionType.ManualPlanning)) {

				result = pRListProcessor.importManualPRList(pSId, manPRList);
			}

			// Save session info
			boolean ack1 = persistPerformer.persistSession(pSId);

			// Save resource values
			boolean ack2 = persistPerformer.persistResources(pSId);

			if (ack1 && ack2) {

				logger.info("Session Info and Resource Values for Planning Session: " + pSId + " saved.");

			} else {

				logger.warn("Session Info and Resource Values for Planning Session: " + pSId + " NOT saved.");

				result = false;
			}

			// Write BRM report file
			RulesPerformer.writeBRMReportFile(pSId);

			if (! SessionScheduler.persistenceMap.containsKey(pSId)) {	
				
				SessionScheduler.persistenceMap.put(pSId, "true");
			}
			
			// Respond Operation Result
			respondOperationResult(pSId, result);
						
			// Added on 22/1/2021 for Sentinel Info message handler
			sendSentinelInfo(pSId,  result);
			
			// Close session elements
			sessionScheduler.closeSession(pSId);

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
		}
	}

	/**
	 * Handle NextAR message from DHM
	 * // TODO: handle SCM Response wait
	 *
	 * @throws JMSException
	 * @throws InputException
	 * @throws IOException
	 * @throws Exception
	 */
	private synchronized void handleNextAR() throws JMSException, InputException, IOException {

		/**
		 * Instance handlers
		 */
		NextARProcessor aRProcessor = new NextARProcessor();
		
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();
		
		/**
		 * The scheduling result
		 */
		boolean result = false;
			
		// Parse message
		NextAR nextAR = NextAR.parseFrom(this.messageHandler.getMessageAsBytes(this.message));

		logger.debug("Received " + this.message.getStringProperty("messageType") + " message from DHM: "
				+ nextAR.toString());

		/**
		 * The Planning Session Id
		 */
		Long pSId = nextAR.getPlanningSessionId();
	
		try {
			
			logger.debug("Session Id: {} UGS Id: {} - PR Id: {} - AR Id: {} - ", 
					pSId, nextAR.getUgsId(), nextAR.getProgRedId(), nextAR.getArId());

			/**
			 * The scheduling PR Id
			 */
			String schedPRId = ObjectMapper.parseDMToSchedPRId(
					nextAR.getUgsId(), nextAR.getProgRedId());
			
			/**
			 * The scheduling AR Id
			 */
			String schedARId = ObjectMapper.parseDMToSchedARId(
					nextAR.getUgsId(), nextAR.getProgRedId(), nextAR.getArId());
			
			if (SessionActivator.scmResWaitMap.get(pSId)) {
			
				logger.info("Reset the SCM Response flag for Planning Session: " + pSId);
				
				// Reset SCM response wait - the response is in cue if the timeout is sufficient
				SessionActivator.scmResWaitMap.put(pSId, false);
			
			} else {
				
				logger.info("Start NextAR scheduling for Planning Session: " + pSId);
			}

			if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedPRId)) {
			
				 // Process Next AR
				 result = aRProcessor.processNextAR(nextAR, false);
	
				 if (! result && Configuration.dynDI2SBool) {
					 
					CSPSSender cspsSender = new CSPSSender(this.session, this.msl);

					 /**
					  * The list of equivalent PRs
					  */
					ArrayList<ProgrammingRequest> equivPRList = equivDTOHandler.handleSingleDI2SRequests(pSId, schedARId);

					if (! cspsSender.sendSingleDI2SRequest(pSId, equivPRList)) {
						
						logger.debug("No DI2S-able requests found relevant to the submitted NextAR for Planning Session: " + pSId);
						
						// Respond NextAR Schedulability Status
						respondNextARStatus(pSId, result);
					}
				 
				 } else {
					 
					 // Respond NextAR Schedulability Status
					 respondNextARStatus(pSId, result);
				 }
			
			} else {
				
				logger.error("No AR associated to the given PR is found!");
				
				// Respond NextAR Schedulability Status
				respondNextARStatus(pSId, false);
			}

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
			
			// Respond NextAR Schedulability Status
			respondNextARStatus(pSId, false);
		}
	}
	
	/**
	 * Respond PRList acknowledge
	 * @param pSId
	 * @param result
	 * @throws JMSException 
	 */
	private synchronized void respondPRListAck(Long pSId, boolean result) throws JMSException {
		
		// Send back PRList acknowledge
		PRListAck.Builder builder = PRListAck.newBuilder().clear()
				.setPlanningSessionId(pSId).setAccepted(result);

		logger.info("Sending " + PRListAck.class.getSimpleName() + " message to DHM.");

		// Send Broadcast message
		logger.debug("Message to be sent: " + builder.toString());
		this.msl.sendBrodcastMessage(createBytesMessage(builder.build()),
				Environment.getConfiguration().getModuleName(), PRListAck.class.getSimpleName());

		logger.info(PRListAck.class.getSimpleName() + " message sent.");
	}

	/**
	 * Respond NextAR schedulability status
	 * @param pSId
	 * @param result
	 * @throws JMSException 
	 */
	private synchronized void respondNextARStatus(Long pSId, boolean result) throws JMSException {
		
		// Send back NextAR Schedulability Status
		NextARSchedulabilityStatus.Builder builder = NextARSchedulabilityStatus.newBuilder().clear()
				.setPlanningSessionId(pSId).setConflictResult(result);

		logger.info("Sending " + NextARSchedulabilityStatus.class.getSimpleName() + " message to BPM.");

		// Send Broadcast message
		logger.debug("Message to be sent: " + builder.toString());
		this.msl.sendBrodcastMessage(createBytesMessage(builder.build()),
				Environment.getConfiguration().getModuleName(), NextARSchedulabilityStatus.class.getSimpleName());

		logger.info(NextARSchedulabilityStatus.class.getSimpleName() + " message sent.");
	}

	/**
	 * Handle DI2S response message from DHM
	 *
	 * @return
	 * @throws JMSException
	 * @throws InvalidProtocolBufferException
	 */
	private synchronized void handleDI2SResult() throws JMSException, InvalidProtocolBufferException {

		/**
		 * Instance handlers
		 */
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();
		
		PersistPerformer persistPerformer = new PersistPerformer();

		SessionScheduler sessionScheduler = new SessionScheduler();

		try {

			// Parse message
			DI2SCompatibilityResult di2SRes = DI2SCompatibilityResult
					.parseFrom(this.messageHandler.getMessageAsBytes(this.message));

			logger.info("Received " + this.message.getStringProperty("messageType") + " message from DHM: "
					+ di2SRes.toString());

			/**
			 * The Planning Session Id
			 */
			Long pSId = di2SRes.getPlanningSessionId();

			/**
			 * The DI2S result
			 */
			boolean di2sResult = false;
			
			// Plan DI2S result
			for (DI2SProgrammingRequestSet di2sPRSet : di2SRes.getDi2SPRSetList()) {

				di2sResult = equivDTOHandler.handleDI2SResult(di2SRes.getPlanningSessionId(), di2sPRSet);
			}
			
			if (Configuration.dynDI2SBool) {
				
				// Respond NextAR status
				respondNextARStatus(pSId, di2sResult);
			
			} else if (!SessionChecker.isUnranked(pSId)
							&& !SessionChecker.isSelf(pSId)) {
			
				// Respond Conflict Report
				respondConflictReport(pSId, false, true);
			
			} else {
				
				/**
				 * The final result
				 */
				boolean result = true;
				
				// Save session info
				logger.info("Persist Session Info for Planning Session: " + pSId);
				boolean ack1 = persistPerformer.persistSession(pSId);

				// Save resource values
				logger.info("Persist Resources for Planning Session: " + pSId);
				boolean ack2 = persistPerformer.persistResources(pSId);

				if (ack1 && ack2) {

					logger.info("Session Info and Resource Values for Planning Session: " + pSId + " saved.");

				} else {

					logger.warn("Session Info and Resource Values for Planning Session: " + pSId + " NOT saved.");

					result = false;
				}

				// Write BRM report file
				RulesPerformer.writeBRMReportFile(pSId);

				if (! SessionScheduler.persistenceMap.containsKey(pSId)) {	
					
					SessionScheduler.persistenceMap.put(pSId, "true");
				}
				
				// Respond Operation Result
				respondOperationResult(pSId, result);
								
				// Added on 22/1/2021 for Sentinel Info message handler
				sendSentinelInfo(pSId,  result);
				
				// Close session elements
				sessionScheduler.closeSession(pSId);
			}

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
		}
	}

	/**
	 * Handle CloseSessionNotification message from DHM
	 *
	 * @return
	 * @throws JMSException
	 * @throws IOException
	 */
	private synchronized void handleCloseSessionNotification() throws JMSException, IOException {

		/**
		 * Instance handlers
		 */
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();

		SubscriptionHandler subscrHandler = new SubscriptionHandler();
		
		PersistPerformer persistPerformer = new PersistPerformer();

		SessionScheduler sessionScheduler = new SessionScheduler();
		
		CSPSSender cspsSender = new CSPSSender(this.session, this.msl);

		try {

			// Parse message
			CloseSessionNotification csn = CloseSessionNotification
					.parseFrom(this.messageHandler.getMessageAsBytes(this.message));
			logger.debug("Received " + this.message.getStringProperty("messageType") + " message from DHM: "
					+ csn.toString());

			/**
			 * The Planning Session Id
			 */
			Long pSId = csn.getPlanningSessionId();
			
			// Set rejected DTOs
			logger.info("Set rejected DTOs for Planning Session: " + pSId);
			sessionScheduler.setRejDTOIds(pSId);

			// Collect the subscription statuses
			if (SessionChecker.isSubscriptionEnabled(pSId)) {

				logger.info("Collect the subscription statuses for Planning Session: " + pSId);
				subscrHandler.collectSubscriptionStatuses(pSId);
			}

			if (! Configuration.dynDI2SBool) {
			
				// Submit the multiple DI2S requests
				logger.info("Check the multiple DI2S-ability for Planning Session: " + pSId);
	
				/**
				 * The list of equivalent PRs for DI2S
				 */
				ArrayList<ProgrammingRequest> equivPRList = equivDTOHandler
						.handleMultiDI2SRequests(pSId);
				
				// Submit multiple DI2S requests
				if (! cspsSender.sendMultiDI2SRequest(pSId, equivPRList)) {
			
					logger.info("No DI2S-able requests found for Planning Session Id: " + pSId);
					
					//Respond Conflict Report
					respondConflictReport(pSId, false, true);				
				}
				
			} else if (SessionChecker.isUnranked(pSId) 
						|| SessionChecker.isSelf(pSId)) {
				
				/**
				 * The final result
				 */
				boolean result = true;

				// Save session info
				logger.info("Persist Session Info for Planning Session: " + pSId);
				boolean ack1 = persistPerformer.persistSession(pSId);

				// Save resource values
				logger.info("Persist Resources for Planning Session: " + pSId);
				boolean ack2 = persistPerformer.persistResources(pSId);

				if (ack1 && ack2) {

					logger.info("Session Info and Resource Values for Planning Session: " + pSId + " saved.");

				} else {

					logger.warn("Session Info and Resource Values for Planning Session: " + pSId + " NOT saved.");

					result = false;
				}

				// Write BRM report file
				RulesPerformer.writeBRMReportFile(pSId);

				if (! SessionScheduler.persistenceMap.containsKey(pSId)) {	
					
					SessionScheduler.persistenceMap.put(pSId, "true");
				}
										
				// Respond Operation Result
				respondOperationResult(pSId, result);
							
				// Added on 22/1/2021 for Sentinel Info message handler
				sendSentinelInfo(pSId,  result);
				
				// Close session elements
				sessionScheduler.closeSession(pSId);

			} else {
				
				//  Respond Conflict Report
				respondConflictReport(pSId, false, true);
			}
			
		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
		}
	}

	/**
	 * Handle Enforce Exclusion Request
	 *
	 * @return
	 * @throws JMSException
	 * @throws IOException
	 */
	private synchronized void handleEnforceExclusionRuleRequest() throws JMSException, IOException {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		// Parse message
		EnforceExclusionRuleRequest err = EnforceExclusionRuleRequest
				.parseFrom(this.messageHandler.getMessageAsBytes(this.message));
		logger.debug("Received " + this.message.getStringProperty("messageType") + " message from DHM: "
				+ err.toString());

		/**
		 * The Planning Session Id
		 */
		Long pSId = err.getPlanningSessionId();

		try {
			
			// Purge the requests
			logger.info("Purge the scheduled requests for Planning Session: " + pSId);
			boolean result = rulesPerformer.purgeSchedTasks(pSId);

			// Send back Enforce Exclusion Rule Result
			EnforceExclusionRuleResult.Builder builder = EnforceExclusionRuleResult.newBuilder().clear()
					.setPlanningSessionId(pSId).setResult(result);

			logger.info("Sending " + EnforceExclusionRuleResult.class.getSimpleName()
					+ "acknowledge message for Planning Session: " + pSId);
			logger.debug("Message to be sent: " + builder.toString());

			// Send Broadcast message
			this.msl.sendBrodcastMessage(createBytesMessage(builder.build()),
					Environment.getConfiguration().getModuleName(), EnforceExclusionRuleResult.class.getSimpleName());

			logger.info(EnforceExclusionRuleResult.class.getSimpleName() + " message sent.");
			System.out.println("");

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
		}
	}

	/**
	 * Handle SetPlanningSessionStatus message from BPM
	 *
	 * @return
	 * @throws JMSException
	 * @throws IOException
	 */
	private synchronized void handleSetPlanningSessionStatus() throws JMSException, IOException {

		/**
		 * Output boolean
		 */
		boolean result = true;

		/**
		 * Instance handlers				
		 */
		PersistPerformer persistPerformer = new PersistPerformer();

		SessionScheduler sessionScheduler = new SessionScheduler();

		// Parse message
		SetPlanningSessionStatus sps = SetPlanningSessionStatus
				.parseFrom(this.messageHandler.getMessageAsBytes(this.message));

		logger.debug("Received " + this.message.getStringProperty("messageType") 
				+ " message from BPM " + sps.toString());

		Long pSId = sps.getPlanningSessionId();

		try {

			// Save persistence
			SessionScheduler.persistenceMap.put(pSId, Boolean.toString(sps.getPersistence()));
				
			// TODO: 29/07/2020 - Moved sessionScheduler.finalizeSchedule(pSId);			
			
			if (sps.getPersistence()) {
				
				// Save session info
				logger.info("Persist Session Info for Planning Session: " + pSId);
				boolean ack1 = persistPerformer.persistSession(pSId);

				// Save resource values
				logger.info("Persist Resources for Planning Session: " + pSId);
				boolean ack2 = persistPerformer.persistResources(pSId);

				if (ack1 && ack2) {

					logger.info("Session Info and Resource Values for Planning Session: " + pSId + " saved.");

				} else {

					logger.warn("Session Info and Resource Values for Planning Session: " + pSId + " NOT saved.");

					result = false;
				}

			} else if (SessionActivator.workPSIdMap.containsKey(pSId)){

				logger.info("No persistence required for Planning Session: " + pSId);
										
				// Save additional info for the not persisted Planning Session
				if (SessionActivator.workPSIdMap.get(pSId) != null)  {
					
					persistPerformer.saveNotPersistingInfo(pSId, SessionActivator.workPSIdMap.get(pSId));
				
				} else {
					
					logger.info("No working Planning Session found associated to Planning Session: " + pSId);
				}
				
				logger.info("No persistence required for Planning Session: " + pSId);
			}
		
			// Write BRM report file
			RulesPerformer.writeBRMReportFile(pSId);
					
			if (! SessionScheduler.persistenceMap.containsKey(pSId)) {	
				
				SessionScheduler.persistenceMap.put(pSId, "true");
			}

			// Respond Operation Result
			respondOperationResult(pSId, result);
						
			// Added on 22/1/2021 for Sentinel Info message handler
			sendSentinelInfo(pSId,  result);
			
			// Close session elements
			sessionScheduler.closeSession(pSId);

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
			
			//Respond Operation Result
			respondOperationResult(pSId, false);
			
		}

		logger.info("***** End of CSPS scheduling for Planning Session: " + pSId);
		
	}

	/**
	 * Respond the Operation Result
	 * @param pSId
	 * @param result
	 * @throws JMSException 
	 */
	private synchronized void respondOperationResult(Long pSId, boolean result) throws JMSException {
		
		try {
		
			// Send back Operation Result
			OperationResult.Builder builder = OperationResult.newBuilder().clear().setPlanningSessionId(pSId)
					.setSuccessful(result)
					.setPlanningSessionStatus(String.valueOf(SessionScheduler.persistenceMap.get(pSId))); // for SPLA-GUI
	
			logger.info("Sending Operation Result acknowledge message for Planning Session: " + pSId);
			logger.debug("Message to be sent: " + builder.toString());
	
			// Send Broadcast message
			this.msl.sendBrodcastMessage(createBytesMessage(builder.build()),
					Environment.getConfiguration().getModuleName(), OperationResult.class.getSimpleName());
	
			logger.info(OperationResult.class.getSimpleName() + " message sent.");
			System.out.println("");
				
			// Purge BRM scheduled Tasks in the Planning Session
			RulesPerformer rulesPerformer = new RulesPerformer();		
			rulesPerformer.purgeSchedTasks(pSId);
			
		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
		}
	}
	
	/**
	 * Respond the Operation Result
	 * @param pSId
	 * @param result
	 * @throws Exception 
	 */
	public synchronized void sendSentinelInfo(Long pSId, boolean result) throws Exception {
		
		logger.info("Sending Sentinel Info message for Planning Session: " + pSId);

		/**
		 * Sentinel Info Handler
		 */
		SentinelInfoHandler sentinelInfoHandler = new SentinelInfoHandler();
		
		// Send back Operation Result
		Builder builder = sentinelInfoHandler.getSentinelInfo(pSId); // for SPLA-GUI

		logger.debug("Message to be sent: " + builder.toString());

		// Send Broadcast message
		this.msl.sendBrodcastMessage(createBytesMessage(builder.build()),
				Environment.getConfiguration().getModuleName(), SentinelInfoMessage.class.getSimpleName());

		logger.info(SentinelInfoMessage.class.getSimpleName() + " message sent.");
		System.out.println("");
		
		// Purge BRM scheduled Tasks in the Planning Session
		RulesPerformer rulesPerformer = new RulesPerformer();		
		rulesPerformer.purgeSchedTasks(pSId);
	}
	
	/**
	 * Handle FilteringResult message from SCM
	 *
	 * @param pSId
	 * @return
	 * @throws JMSException
	 * @throws IOException
	 */
	private synchronized void handleFilteringResult() throws JMSException, IOException {

		/**
		 *  Instance handlers				
		 */
		FilterDTOHandler filtDTOHandler = new FilterDTOHandler();
		
		SessionScheduler sessionScheduler = new SessionScheduler();

		try {

			// Parse message
			FilteringResult filtRes = FilteringResult.parseFrom(this.messageHandler.getMessageAsBytes(this.message));
			logger.debug("Received " + this.message.getStringProperty("messageType") + " message from SCM: "
					+ filtRes.toString());

			/**
			 * The Planning Session Id
			 */
			Long pSId = filtRes.getPlanningSessionId();
			// Handle rejected requests with rejected subscribers
			if (SessionActivator.scmAvailMap != null 
				&& SessionActivator.scmAvailMap.get(pSId) != null) {
			
				// handle rejected requests
				filtDTOHandler.handleRejRequests(pSId, filtRes.getRejectedRequestsList());
				
				// Set SCM availability
				SessionActivator.scmAvailMap.put(pSId, true);
				
				// Set SCM response wait
				SessionActivator.scmResWaitMap.put(pSId, false);
			}

			logger.debug("Analysis of the Filtering Result completed for Planning Session: " + pSId);

			// The Self-Generated case
			if (SessionChecker.isSelf(pSId)) {
			
				boolean result = true;
				
				PersistPerformer persistPerformer = new PersistPerformer();
				
				// Save session schedule info
				logger.info("Persist Session Info for Planning Session: " + pSId);
				boolean ack1 = persistPerformer.persistSession(pSId);

				// Save resource values
				logger.info("Persist Resources for Planning Session: " + pSId);
				boolean ack2 = persistPerformer.persistResources(pSId);

				if (ack1 && ack2) {

					logger.info("Session Info and Resource Values for Planning Session: " + pSId + " saved.");

				} else {

					logger.warn("Session Info and Resource Values for Planning Session: " + pSId + " NOT saved.");

					result = false;
				}
				
				// Write BRM report file
				RulesPerformer.writeBRMReportFile(pSId);
				
				if (! SessionScheduler.persistenceMap.containsKey(pSId)) {	
					
					SessionScheduler.persistenceMap.put(pSId, "true");
				}
				
				// Respond Operation Result
				respondOperationResult(pSId, result);
								
				// Added on 22/1/2021 for Sentinel Info message handler
				sendSentinelInfo(pSId,  result);
								
				// Close session elements			 
				sessionScheduler.closeSession(pSId);
			}
		
		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());
		}
	}

	/**
	 * Respond Conflict Report
	 * 
	 * @param pSId
	 * @return
	 * @throws Exception
	 */
	private synchronized boolean respondConflictReport(Long pSId, boolean isAsync, boolean result) 
			throws Exception {

		logger.info("Respond Conflict Report for Planning Session " + pSId);
		
		/**
		 *  Instance handlers				
		 */		
		PRListProcessor pRListProcessor = new PRListProcessor();
		
		SessionScheduler sessionScheduler =  new SessionScheduler();
		
		SentinelInfoHandler sentinelInfoHandler = new SentinelInfoHandler();

		try {
			
			// Added on 07/06/2022 for async response
			if (!isAsync) {
				
				result = sessionScheduler.finalizeSchedule(pSId);		
			}
			
			/**
			 * The list of Plan Request Status
			 */
			ArrayList<PlanProgrammingRequestStatus> pSPRStatuses = pRListProcessor.getPlanSessionPRStatuses(pSId);
			
			logger.debug("Planning PR Statuses sent for Planning Session: " + pSId); 
			logger.debug(pSPRStatuses.toString());

			/**
			 * The list of Plan Subscribing Request Status
			 */
			List<PlanSubscribingRequestStatus> pSSubPRStatuses = new ArrayList<PlanSubscribingRequestStatus>();
					
			if (! isAsync) {
			
				pSSubPRStatuses = pRListProcessor.getPlanSessionSubPRStatuses(pSId);

				logger.debug("Subscribing PR Statuses sent for Planning Session: " + pSId + ": "); 
				logger.debug(pSSubPRStatuses.toString());
			}
		
			/**
			 * The planning input SENTINEL PR Id List
			 */ 
			ArrayList<String> planningSentPRIdList = sentinelInfoHandler.getInputSentinelIds(pSId);
			
			/**
			 * The planned output SENTINEL PR Id List
			 */
			ArrayList<String> plannedSentPRIdList = sentinelInfoHandler.getOutputSentinelIds(pSId);
			
			/**
			 * The synchronous conflict report builder
			 * // Updated minor on 26/04/2022
			 */
			ConflictReport.Builder builder = ConflictReport.newBuilder().clear()
					.setPlanningSessionId(pSId).setReport(MessageHandler.serializeByteString(pSPRStatuses))
					.setSubscribingReport(MessageHandler.serializeByteString(pSSubPRStatuses))
					.setSavedProgrammingRequestList(MessageHandler.serializeByteString(pRListProcessor.getPlanSessionPRList(pSId)))
					.setResult(result).setPlanningSentinels(planningSentPRIdList.size()).setPlannedSentinels(plannedSentPRIdList.size());
			
			/**
			 * The quota builder
			 */
			ConflictReport.QuotaInfo.Builder quotaBuilder = ConflictReport.QuotaInfo.newBuilder();

			logger.info("Compute partners quota.");

			for (Partner partner : SessionActivator.partnerListMap.get(pSId)) {

				quotaBuilder.setPartner(partner.getId());

				logger.debug("Compute quota for partner: " + partner.getId());

				quotaBuilder.setQuota(BICCalculator.getConsumedQuota(pSId, partner.getUgsId()));

				builder.addPartnersQuota(quotaBuilder);
			}

			logger.info("Sending " + ConflictReport.class.getSimpleName()
					+ " message to DHM/BPM for Planning Session: " + pSId);

			// Send conflict report
			logger.debug("Message to be sent: " + builder.toString());
			this.msl.sendBrodcastMessage(createBytesMessage(builder.build()),
					Environment.getConfiguration().getModuleName(), ConflictReport.class.getSimpleName());

			logger.info(ConflictReport.class.getSimpleName() + " message sent.");
			System.out.println("");
			
		} catch (Exception e) {

			logger.error("Exception raised: " + e.getLocalizedMessage());

			result = false;
		}

		return (result);
	}
	

	/**
	 * Respond Unranked AR Schedulability status
	 * 
	 * @param pSId - the Planning Session Id
	 * @throws Exception 
	 */
	private synchronized void respondUnrankedStatus(Long pSId) throws Exception {
			
		// Return UnrankedARSchedulabilityStatus to DHM
		UnrankedARSchedulabilityStatus.Builder builder = UnrankedARSchedulabilityStatus.newBuilder().clear()
				.setPlanningSessionId(pSId).setNumberScheduledAR(PRListProcessor.getSchedARNumber(pSId));

		logger.debug("Sending " + UnrankedARSchedulabilityStatus.class.getSimpleName() + " message to DHM.");
		logger.debug("Message to be sent: " + builder.toString());

		this.msl.sendBrodcastMessage(createBytesMessage(builder.build()),
				Environment.getConfiguration().getModuleName(),
				UnrankedARSchedulabilityStatus.class.getSimpleName());

		logger.info(UnrankedARSchedulabilityStatus.class.getSimpleName() + " message sent.");

	}

	/**
	 * Handle SCMCheck Result message from DHM
	 * // TODO: add Planning Session Id to the SCM Check	   
	 * @throws InvalidProtocolBufferException
	 * @throws JMSException
	 */
	private synchronized void handleSCMCheckResult() throws InvalidProtocolBufferException, JMSException {

		// Parse message
		SCMCheckResult scmCheckRes = SCMCheckResult.parseFrom(this.messageHandler.getMessageAsBytes(this.message));

		logger.debug("Received " + this.message.getStringProperty("messageType") + " message from DHM: "
				+ scmCheckRes.toString());
		
		/**
		 * The Planning Session Id
		 */
		Long pSId = scmCheckRes.getPlanningSessionId();

		logger.debug("Received SCM Check Result for PlanningSession: " + pSId);
		
		SessionActivator.scmAvailMap.put(pSId, scmCheckRes.getResult());
		
		if (! scmCheckRes.getResult()) {
			
			logger.warn("SCM Check Result NOT OK for PlanningSession: " + pSId);

			if (SessionActivator.scmResWaitMap.containsKey(pSId)) {
			
				SessionActivator.scmResWaitMap.put(pSId,  false);
			}
			
		} else {
				
			logger.info("SCM Check Result OK for PlanningSession: " + pSId);
		}		
	}

	/**
	 * Create Bytes Message
	 *
	 * @param msg
	 * @return
	 * @throws JMSException
	 */
	protected BytesMessage createBytesMessage(GeneratedMessage msg) throws JMSException {

			/**
		 * The byte message
		 */
		BytesMessage byteMessage = this.session.createBytesMessage();

		byte[] message = msg.toByteArray();

		// Write object
		byteMessage.writeObject(message);

		return byteMessage;
	}	

}
