package org.wsdmcup17.demo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

/**
 * Thread to demultiplex revisions and meta data. The resulting revisions are
 * provided as an {@link OutputStream} that can, for example, be processed with
 * Wikidata Toolkit. The metadata is parsed and put in a queue for further
 * processing.
 */
public class Demultiplexer implements Runnable {
	
	private static final Logger
		LOG = Logger.getLogger(Demultiplexer.class);
	
	private static final String
		LOG_MSG_END_OF_ITEM_STREAM = "End of item stream.",
		LOG_MSG_MISSING_DATA_AFTER_METADATA = "Missing data after metadata %s";
	
	private DataInputStream dataStream;
	private CSVPrinter resultPrinter;
	
	public Demultiplexer(
		DataInputStream inputStream,CSVPrinter resultPrinter
	) {
		this.dataStream = inputStream;
		this.resultPrinter = resultPrinter;
	}
	
	@Override
	public void run() {
		try {
			demultiplexStream();
		}
		catch (Throwable e) {
			LOG.error("", e);
		}
	}

	private void demultiplexStream() throws IOException, InterruptedException {
		
		Process proc = Runtime.getRuntime().exec("python /home/barberry/programs/testworker.py");
		final BufferedReader bre = new BufferedReader
		        (new InputStreamReader(proc.getErrorStream()));



		new Thread(new Runnable() {    
		    @Override
		    public void run() {
				String s = "";
		       try {
				while ((s = bre.readLine()) != null) {
				        LOG.error(s);
				    }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		     }
		 }).start();

		RandomAccessFile pipe = new RandomAccessFile(
		        "/home/barberry/data/notify", "r");

		String res = pipe.readLine();
		pipe.close();
		try {
			while (true) {
				// Read metadata from stream and queue it.
				byte[] bytes1 = readNextItem(dataStream);
				if (bytes1 == null) { // end of stream
					break;
				}


				
				// Read corresponding revision from stream and forward it.
				byte[] bytes2 = readNextItem(dataStream);
				if (bytes2 == null) { // end of stream
					LOG.error("shabi");
					break;
				}

				LOG.info("Writing revision and metadata...");
				File revisionFile = new File("/home/barberry/data/revision");
				OutputStream ofStream = new FileOutputStream(revisionFile);
				File metadataFile = new File("/home/barberry/data/metadata");
				OutputStream mdStream = new FileOutputStream(metadataFile);
				mdStream.write(bytes1);
				mdStream.flush();
				ofStream.write(bytes2);
				ofStream.flush();
				ofStream.close();
				mdStream.close();
				classifyRevision(resultPrinter);
			}
		}
		finally {
			LOG.info("closing resultPrinter");
			resultPrinter.close();
		}
	}
	
	private float classifyRevision(CSVPrinter resultPrinter) {
		// This is where an actual classification based on  the revision and
		// its associated metadata should happen. Instead, we just assign a
		// score of 0.0, effectively classifying the revision as non-vandalism.
		Process proc;
		try {
			LOG.info("Extracting Features...");
			proc = Runtime.getRuntime().exec("java -jar /home/barberry/programs/featureextractor.jar --revisiontags /home/barberry/data/metadata /home/barberry/data/revision /home/barberry/data/feature");
			proc.waitFor();
			
			RandomAccessFile pipe = new RandomAccessFile(
			        "/home/barberry/data/notify", "rw");
			    String req = "datahasarrived";
			    // Write request to the pipe
			    pipe.write(req.getBytes());
			    pipe.close();
			    pipe =new RandomAccessFile(
				        "/home/barberry/data/notify", "r");
			    // Read response from pipe
			    LOG.info("Waiting for python response...");
			    String res = pipe.readLine();
			    pipe.close();
			    if(res != null && !res.equals("")){
			    	LOG.info("Retreiving results...");
			    	BufferedReader br = new BufferedReader(new FileReader("/home/barberry/data/result"));
			    	try {

			    	    String line = br.readLine();
			    	    while (line != null) {
			    	    	String[] content = line.split(" ");
			    	    	sendClassificationResult(Long.parseLong(content[0]), Float.valueOf(content[1]), resultPrinter);
			    	        line = br.readLine();
			    	    }
			    	    
			    	} finally {
			    	    br.close();
			    	}
			    }

		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error("", e);
		}
	    
		return 0.0f;
	}

	private void sendClassificationResult(
		long revisionId, float classificationScore, CSVPrinter resultPrinter
	) {
		LOG.info("Sending results...: " + revisionId + ", " + classificationScore );
		try {
			resultPrinter.print(revisionId);
			resultPrinter.print(classificationScore);
			resultPrinter.println();
			resultPrinter.flush();
		}
		catch (IOException e) {
			LOG.error("error while writting results", e);
		}
	}
	private static byte[] readNextItem(DataInputStream dataStream)
	throws IOException {
		try {
			int length = dataStream.readInt();
			byte[] bytes = new byte[length];
			dataStream.readFully(bytes);
			return bytes;
		}
		catch (EOFException e) {
			LOG.info(LOG_MSG_END_OF_ITEM_STREAM);
			return null;
		}
	}
	
}
