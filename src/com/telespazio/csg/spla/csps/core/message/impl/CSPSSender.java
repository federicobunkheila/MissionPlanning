/**
*
* MODULE FILE NAME: CSPSSender.java
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

import java.util.ArrayList;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;
import com.nais.spla.msl.main.core.MessageServiceLayer;
import com.telespazio.csg.spla.csps.core.server.Environment;
import com.telespazio.csg.spla.csps.handler.MessageHandler;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityRequest;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityRequest.DI2SProgrammingRequest;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityRequest.DI2SProgrammingRequestSet;
import com.telespazio.splaif.protobuf.EnforceExclusionRuleMessage.EnforceExclusionRuleRequest;
import com.telespazio.splaif.protobuf.EnforceExclusionRuleMessage.EnforceExclusionRuleResult;
import com.telespazio.splaif.protobuf.FilteringMessage.FilteringRequest;
import com.telespazio.splaif.protobuf.StatusCheckmessage.SCMCheck;

import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;

/**
 * The CSPS Sender class.
 */
public class CSPSSender {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(CSPSSender.class);

	/**
	 * The message service layer
	 */
	private MessageServiceLayer msl;

	/**
	 * The javax session
	 */
	private Session session;

	/**
	 * The CSPS Sender
	 * 
	 * @param session
	 * @param msl
	 */
	public CSPSSender(Session session, MessageServiceLayer msl) {

		super();

		// Instance msl
		this.msl = msl;
		
		// Instance session
		this.session = session;

	}

	/**
	 * Send the SCM check message
	 *
	 * @param pSId
	 */
	public synchronized void sendSCMCheck(Long pSId) {

		try {

			logger.info("Query about SCM availability for Planning Session: " + pSId);
			
			/**
			 * The SCM Check builder
			 */
			SCMCheck.Builder scmBuilder = SCMCheck.newBuilder();
			scmBuilder.setPlanningSessionId(pSId);

			// Send message
			send(scmBuilder.build(), SCMCheck.class.getSimpleName());

			logger.info("Sending " + SCMCheck.class.getSimpleName() + " message to SCM.");
			logger.info("Message to be sent: " + scmBuilder.toString());

		} catch (Exception e) {

			logger.error("Error managing incoming message to MSL: " + e.getMessage(), e);
		}
	}

	/**
	 * Send multiple DI2S request
	 *
	 * @param pSId
	 * @param equivPRList
	 */
	public synchronized boolean sendMultiDI2SRequest(Long pSId, ArrayList<ProgrammingRequest> equivPRList) {

		/**
		 * The boolean for DI2S message
		 */
		boolean isDI2SBool = false;

		try {
						
			if (equivPRList != null && ! equivPRList.isEmpty()) {
				
				isDI2SBool = true;
				
				logger.debug("A list of DI2S requests is submitted for Planning Session: " + pSId);

				logger.info("A number of " + (equivPRList.size() / 2.0)
						+ " couples of matching DI2S requests submitted to SPARC through DHM.");
	
				// Submit DI2S requests to SPARC
				logger.debug("Submit the set of DI2S requests for Planning Session: " + pSId);
	
				// DI2SCompatibilityRequest to DHM
				DI2SCompatibilityRequest.Builder di2sCompBuilder = DI2SCompatibilityRequest.newBuilder().clear()
						.setPlanningSessionId(pSId);
	
				logger.trace("Build request message...");
	
				for (int i = 0; i < (int) ((double) equivPRList.size() / 2.0); i++) {
	
					/**
					 * The DI2S PRSet builder
					 */
					DI2SProgrammingRequestSet.Builder di2sPRSetBuilder = DI2SProgrammingRequestSet.newBuilder().clear();
	
					logger.trace("Build first DI2S PR.");
	
					/**
					 * The 1st DI2S PR builder
					 */
					DI2SProgrammingRequest.Builder di2sPRBuilder1 = DI2SProgrammingRequest.newBuilder().clear()
							.setProgrammingRequest(MessageHandler.serializeByteString(equivPRList.get(2 * i)));
	
					di2sPRSetBuilder.setDi2SPRs1(di2sPRBuilder1)
							.setPrSetId(equivPRList.get((2 * i)).getProgrammingRequestId());
	
					logger.trace("Build second DI2S PR.");
	
					/**
					 * The 2nd DI2S PR builder
					 */
					DI2SProgrammingRequest.Builder di2sPRBuilder2 = DI2SProgrammingRequest.newBuilder().clear()
							.setProgrammingRequest(MessageHandler.serializeByteString(equivPRList.get((2 * i) + 1)));
	
					di2sPRSetBuilder.setDi2SPRs2(di2sPRBuilder2)
							.setPrSetId(equivPRList.get((2 * i)).getProgrammingRequestId());
	
					di2sCompBuilder.addDi2SPRSet(di2sPRSetBuilder);
				}
	
				logger.info("Sending " + DI2SCompatibilityRequest.class.getSimpleName() + " message to DHM.");
				logger.info("Message to be sent: " + di2sCompBuilder.toString());
	
				// Send DI2S Result
				send(di2sCompBuilder.build(), DI2SCompatibilityRequest.class.getSimpleName());
	
				logger.info(DI2SCompatibilityRequest.class.getSimpleName() + " message sent.");
				System.out.println("");
			
			} else {
				
				logger.debug("No DI2S requests to be submitted for Planning Session: " + pSId);
			}

		} catch (Exception e) {

			logger.error("Error managing outcoming message to MSL: " + e.getMessage(), e);
		}
		
		return isDI2SBool;
	}
	
	/**
	 * Send single DI2S request
	 *
	 * @param pSId
	 * @param equivPRList
	 */
	public synchronized boolean sendSingleDI2SRequest(Long pSId, ArrayList<ProgrammingRequest> equivPRList) {

		/**
		 * The boolean for DI2S message
		 */
		boolean isDI2SBool = false;

		try {
						
			if (equivPRList != null && ! equivPRList.isEmpty()) {
				
				isDI2SBool = true;
				
				logger.debug("A list of DI2S requests is submitted for Planning Session: " + pSId);

				logger.info("A number of " + (equivPRList.size() / 2.0)
						+ " couples of matching DI2S requests submitted to SPARC through DHM.");
	
				// Submit DI2S requests to SPARC
				logger.debug("Submit the set of DI2S requests for Planning Session: " + pSId);
	
				// DI2SCompatibilityRequest to DHM
				DI2SCompatibilityRequest.Builder di2sCompBuilder = DI2SCompatibilityRequest.newBuilder().clear()
						.setPlanningSessionId(pSId);
	
				logger.trace("Build request message...");
	
				for (int i = 0; i < (int) ((double) equivPRList.size() / 2.0); i++) {
	
					/**
					 * The DI2S PRSet builder
					 */
					DI2SProgrammingRequestSet.Builder di2sPRSetBuilder = DI2SProgrammingRequestSet.newBuilder().clear();
	
					logger.trace("Build first DI2S PR.");
	
					/**
					 * The 1st DI2S PR builder
					 */
					DI2SProgrammingRequest.Builder di2sPRBuilder1 = DI2SProgrammingRequest.newBuilder().clear()
							.setProgrammingRequest(MessageHandler.serializeByteString(equivPRList.get(2 * i)));
	
					di2sPRSetBuilder.setDi2SPRs1(di2sPRBuilder1)
							.setPrSetId(equivPRList.get((2 * i)).getProgrammingRequestId());
	
					logger.trace("Build second DI2S PR.");
	
					/**
					 * The 2nd DI2S PR builder
					 */
					DI2SProgrammingRequest.Builder di2sPRBuilder2 = DI2SProgrammingRequest.newBuilder().clear()
							.setProgrammingRequest(MessageHandler.serializeByteString(equivPRList.get((2 * i) + 1)));
	
					di2sPRSetBuilder.setDi2SPRs2(di2sPRBuilder2)
							.setPrSetId(equivPRList.get((2 * i)).getProgrammingRequestId());
	
					di2sCompBuilder.addDi2SPRSet(di2sPRSetBuilder);
				}
	
				logger.info("Sending " + DI2SCompatibilityRequest.class.getSimpleName() + " message to DHM.");
				logger.info("Message to be sent: " + di2sCompBuilder.toString());
	
				// Send DI2S Result
				send(di2sCompBuilder.build(), DI2SCompatibilityRequest.class.getSimpleName());
	
				logger.info(DI2SCompatibilityRequest.class.getSimpleName() + " message sent.");
				System.out.println("");
			
			} else {
				
				logger.debug("No DI2S requests to be submitted for Planning Session: " + pSId);

			}

		} catch (Exception e) {

			logger.error("Error managing outcoming message to MSL: " + e.getMessage(), e);
		}
		
		return isDI2SBool;
	}

	/**
	 * Send DTO filtering message to SCM
	 *
	 * @param pSId
	 *            - the Planning Session Id
	 * @param scmPRList
	 *            - the PRList to be shuttered
	 * @param mhStart
	 *            - the MH start date
	 */
	public synchronized void sendFilteringRequest(Long pSId, ArrayList<ProgrammingRequest> scmPRList, String mhStart) {

		try {

			logger.debug("Send SCM Veto filtering request for Planning Session: " + pSId);

			/**
			 * The Filtering Request
			 */
			FilteringRequest.Builder builder = FilteringRequest.newBuilder().clear().setPlanningSessionId(pSId)
					.setReferenceTime(mhStart).setProgrammingRequest(MessageHandler.serializeByteString(scmPRList));

			// Send Filtering Request
			send(builder.build(), FilteringRequest.class.getSimpleName());

			logger.info("Sending " + FilteringRequest.class.getSimpleName() + " message to DHM.");
			logger.info("Message to be sent: " + builder.toString());

//			logger.debug("Session Id: {} - Programming Request: {}", builder.getPlanningSessionId(),
//					builder.getProgrammingRequest().toString());
			
		} catch (Exception e) {

			logger.error("Error managing outcoming message to MSL: " + e.getMessage(), e);
		}
	}

	/**
	 * Handle enforce exclusion rule result message to BRM
	 *
	 * @param pSId
	 *            - the Planning Session Id
	 * @param result
	 *            - the result boolean
	 * @param mhStart
	 *            - the MH starting date
	 */
	public synchronized void sendEnforceExclusionRuleResult(Long pSId, boolean result) {

		try {

			/** 
			 * The Enforce Exclusion Rule Result
			 */
			EnforceExclusionRuleResult.Builder builder = EnforceExclusionRuleResult.newBuilder().clear()
					.setPlanningSessionId(pSId).setResult(result);

			logger.info("Sending " + EnforceExclusionRuleRequest.class.getSimpleName() + " message to DHM.");
			logger.info("Message to be sent: " + builder.toString());

		} catch (Exception e) {

			logger.error("Error managing outcoming message to MSL: " + e.getMessage(), e);
		}
	}

	/**
	 * Send a message to the ActiveMQ queue encoding the message as ByteMessage
	 *
	 * @param msg
	 *            the msg
	 * @param messageClass
	 *            the message class
	 * @throws JMSException
	 *             the JMS exception
	 */
	private void send(GeneratedMessage msg, String messageClass) throws JMSException {

		// no need for caller to register out message class
		this.msl.sendBrodcastMessage(createBytesMessage(msg), Environment.getConfiguration().getModuleName(),
				messageClass);

		// log message on caller logger if specified
		logger.info("Sent {} message \n\n{}", messageClass, msg.toString());
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
