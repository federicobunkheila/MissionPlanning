
package com.telespazio.csg.spla.csps.handler;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import com.google.protobuf.ByteString;

import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;

/**
 * Handler of the message
 *
 * @author bunkheila
 *
 */
public class MessageHandler {

	/**
	 * Serialize the object using protocol buffer output stream to avoid double
	 * copying of serialized data from a byte buffer to a ByteString
	 *
	 * @param obj
	 * @param expectedSize
	 * @return
	 * @throws IOException
	 */
	public static ByteString serializeByteString(Object obj) throws IOException {
		
		/**
		 * The output Bytestring
		 */		
		ByteString.Output bufOut = ByteString.newOutput();
		ObjectOutputStream out = new ObjectOutputStream(bufOut);
		
		// Write object
		out.writeObject(obj);
		out.flush();
		out.close();

		return bufOut.toByteString();
	}

	// /**
	// * Deserialize a byteString into an object
	// *
	// * @param serializedFormat
	// * @return
	// * @throws IOException
	// * @throws ClassNotFoundException
	// */
	// public static Object deserializeByteStrings(ByteString byteString) throws
	// IOException, ClassNotFoundException
	// {
	// InputStream is = new StringBufferInputStream(byteString.toStringUtf8());
	// BufferedInputStream bis = new BufferedInputStream(is);
	// ObjectInputStream ois = new ObjectInputStream(is);
	// Object obj = ois.readObject();
	// ois.close();
	// return obj;
	// }

	/**
	 * Deserialize a byteString into an object
	 *
	 * @param serializedFormat
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object deserializeByteArray(byte[] serializedFormat) throws IOException, ClassNotFoundException {

		/**
		 * The deserialized object
		 */
		Object obj = new ArrayList<>();
		
		try {
			
			/**
			 * The inpyut stream
			 */
			ByteArrayInputStream in = new ByteArrayInputStream(serializedFormat);
			ObjectInputStream is = new ObjectInputStream(in);
			
			// Read object
			obj = is.readObject();
			is.close();
			
		} catch (Exception ex) {
 
			ex.toString();
		}
		
		return obj;

	}

	/**
	 * 
	 * @param byteString
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object deserializeInputString(ByteString byteString) throws ClassNotFoundException, IOException {
	
		/**
		 * The deserialized object
		 */
		Object obj = new ArrayList<>();
		
		try {
			
			/**
			 * The input stream
			 */
			ByteArrayInputStream bis = new ByteArrayInputStream(byteString.toByteArray());			
			ObjectInput in = new ObjectInputStream(bis);
			
			// Read object
			obj = in.readObject();
	
		} catch (Exception ex) {
			 
			ex.toString();
		}

		return obj;
	}

	/**
	 * Get the message as bytes. Convert a JMS message into a byte array
	 *
	 * @param message
	 *            the message
	 * @return the message as bytes
	 * @throws JMSException
	 *             the JMS exception
	 */
	public byte[] getMessageAsBytes(Message message) throws JMSException {

		/**
		 * The key bytes message
		 */
		byte[] keyBytes;
		BytesMessage bytesMessage = (BytesMessage) message;
		keyBytes = new byte[(int) bytesMessage.getBodyLength()];
		
		// Read bytes
		bytesMessage.readBytes(keyBytes);
		return keyBytes;
	}

	/**
	 * Serialize object
	 *
	 * @param obj
	 * @param expectedSize
	 * @return
	 * @throws IOException
	 */
	public static ByteString serializeObject(Object obj, int expectedSize) throws IOException {
		// Serialize the object using protocol buffer output stream to to avoid
		// double copying of serialized data from a byte buffer to a ByteString
		ByteString.Output bufOut = ByteString.newOutput(expectedSize);
		ObjectOutputStream out = new ObjectOutputStream(bufOut);
		
		// Write object
		out.writeObject(obj);
		out.close();
		return bufOut.toByteString();
	}

	/**
	 * Serialize Object to File
	 * 
	 * @param obj
	 * @param outputFile
	 * @throws IOException
	 */
	public static void serializeObjectToFile(Object obj, String outputFile) throws IOException
	{
		try
		{
			/**
			* The output stream
			*/
			FileOutputStream fos = new FileOutputStream(outputFile);

			// Write object to disk
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			
			// Write object
			oos.writeObject(obj);

			oos.close();
			fos.close();
		}
		catch (Throwable t)
		{
			throw new IOException("Error writing object to file : " + t.getMessage(), t);
		}
	}
	
	/**
	 * Serialize Object to File
	 * 
	 * @param obj
	 * @param inputFile
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static ArrayList<ProgrammingRequest> serializeObjectFromFile(String inputFile) throws IOException
	{
		
		Object pRList = null;
		
		try
		{
			/**
			* The input stream
			*/
			FileInputStream fis = new FileInputStream(inputFile);

			// Write object to disk
			ObjectInputStream ois = new ObjectInputStream(fis);
			
			// Write object
			pRList =  ois.readObject();

			ois.close();
			fis.close();
		}
		catch (Throwable t)
		{
			throw new IOException("Error writing object to file : " + t.getMessage(), t);
		}
		
		return (ArrayList<ProgrammingRequest>) pRList;
	}

}
