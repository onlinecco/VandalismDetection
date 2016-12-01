package org.wsdmcup17.demo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.wikidata.wdtk.dumpfiles.DumpContentType;
import org.wikidata.wdtk.dumpfiles.MwDumpFile;
import org.wikidata.wdtk.dumpfiles.MwDumpFileProcessor;
import org.wikidata.wdtk.dumpfiles.MwLocalDumpFile;
import org.wikidata.wdtk.dumpfiles.MwRevision;
import org.wikidata.wdtk.dumpfiles.MwRevisionDumpFileProcessor;
import org.wikidata.wdtk.dumpfiles.MwRevisionProcessor;

public class Client {
	
	private static final Logger
		LOG = Logger.getLogger(Client.class);
	
	private static final String
		LOG_MSG_CONNECTING_TO = "Connecting to %s",
		CRLF = "\r\n",
		URI_PROTOCOL_TCP = "tcp://",
		THREAD_NAME_DEMULTIPLEXER = "Demultiplexer",
		MW_TOOLKIT_INPUT_STREAM = "INPUT STREAM";
	
	private static final String[]
		RESULT_CSV_HEADER = { "REVISION_ID", "VANDALISM_SCORE" };
	
	private static final CSVFormat
		CSV_FORMAT = CSVFormat.RFC4180.withHeader(RESULT_CSV_HEADER);

	private static final int
		PIPE_SIZE = 32 * 1024 * 1024,
		QUEUE_CAPACITY = 128;
	
	private String serverAddress;
	private String accessToken;
	
	public Client(String serverAddress, String accessToken) {
		this.serverAddress = serverAddress;
		this.accessToken = accessToken;
	}
	
	public void start()
	throws UnknownHostException, IOException, URISyntaxException,
	InterruptedException {
		LOG.info(String.format(LOG_MSG_CONNECTING_TO, serverAddress));
		try (
			Socket socket = createSocket(serverAddress);
			// Multiplexed revision and metadata stream from server.
			InputStream dataStreamPlain = socket.getInputStream();
			// Result stream to server.
			OutputStream resultStreamPlain = socket.getOutputStream();
		) {
			// First send the access token to server to authenticate.
			resultStreamPlain.write((accessToken + CRLF).getBytes());
			resultStreamPlain.flush();

			// Assuming the token is accepted, proceed to process revisions.
			processRevisions(dataStreamPlain, resultStreamPlain);
		}
	}

	private void processRevisions(
		InputStream dataStreamPlain, OutputStream resultStreamPlain
	) throws IOException, InterruptedException {
		// Wrap the streams for convenient usage.
		try (
			DataInputStream dataStream = new DataInputStream(dataStreamPlain);
			Writer resultWriter = new OutputStreamWriter(resultStreamPlain);
			CSVPrinter resultPrinter = new CSVPrinter(resultWriter, CSV_FORMAT);
		) {
			processRevisions(dataStream, resultPrinter);
		}
	}

	private void processRevisions(
		DataInputStream dataStream, CSVPrinter resultPrinter
	) throws IOException, InterruptedException {
		try (
			// Pipes to forward the revision stream to the revision processor.
			PipedOutputStream revisionOutputStream = new PipedOutputStream();
			PipedInputStream revisionInputStream =
				new PipedInputStream(revisionOutputStream, PIPE_SIZE);	
		) {
			// Queue that stores metadata for revisions received from server.
			BlockingQueue<CSVRecord> metadataQueue =
					new ArrayBlockingQueue<>(QUEUE_CAPACITY);

			File revisionFile = new File("~/data/revision");
			OutputStream ofStream = new FileOutputStream(revisionFile);
			File metadataFile = new File("~/data/metadata");
			OutputStream mdStream = new FileOutputStream(revisionFile);
			// Thread that demultiplexes the data stream from the server,
			// writing revisions to the output stream and metadata to the queue.
			Thread demultiplexerThread = createDemultiplexerThread(
					dataStream, ofStream, mdStream);
			/*
			// Event-driven revision processor based on Wikidata toolkit.
			MwDumpFileProcessor revisionProcessor = createRevisionProcessor(
					resultPrinter, metadataQueue);
			
			// Start processing revisions.
			// Note: the processor closes the stream and thus the socket.
			revisionProcessor.processDumpFileContents(
					revisionInputStream, createMwDumpFile());
			*/

			classifyRevision(resultPrinter);
			// Wait for the demultiplexer thread to terminate.
			demultiplexerThread.join();
		}
	}

	private float classifyRevision(CSVPrinter resultPrinter) {
		// This is where an actual classification based on  the revision and
		// its associated metadata should happen. Instead, we just assign a
		// score of 0.0, effectively classifying the revision as non-vandalism.
		Process proc;
		try {
			proc = Runtime.getRuntime().exec("java -jar ~/programs/featureextractor.jar --revisiontags ~/data/metadata ~/data/revision ~/data/feature");
			proc.waitFor();
			
			RandomAccessFile pipe = new RandomAccessFile(
			        "~/data/notify", "rw");
			 
			    String req = "datahasarrived";
			    // Write request to the pipe
			    pipe.write(req.getBytes());
			 
			    // Read response from pipe
			    String res = pipe.readLine();
			    if(res != null && !res.equals("")){

			    	BufferedReader br = new BufferedReader(new FileReader("~/data/result"));
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
			    // Close the pipe
			    pipe.close();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
		return 0.0f;
	}

	private void sendClassificationResult(
		long revisionId, float classificationScore, CSVPrinter resultPrinter
	) {
		try {
			resultPrinter.print(revisionId);
			resultPrinter.print(classificationScore);
			resultPrinter.println();
			resultPrinter.flush();
		}
		catch (IOException e) {
			LOG.error("", e);
		}
	}
	private MwDumpFileProcessor createRevisionProcessor(
		CSVPrinter resultPrinter, BlockingQueue<CSVRecord> metadataQueue
	) {
		MwRevisionProcessor revisionProcessor =
			new DummyRevisionClassifier(metadataQueue, resultPrinter);
		return new MwRevisionDumpFileProcessor(revisionProcessor);
	}

	private Thread createDemultiplexerThread(
		DataInputStream dataStream,	OutputStream revisionOutputStream,
		OutputStream metadataQueue
	) {
		Demultiplexer d = new Demultiplexer(
				dataStream, metadataQueue, revisionOutputStream);
		Thread demultiplexerThread = new Thread(d, THREAD_NAME_DEMULTIPLEXER);
		demultiplexerThread.start();
		return demultiplexerThread;
	}

	private MwDumpFile createMwDumpFile() {
		MwLocalDumpFile mwDumpFile = new MwLocalDumpFile(
				MW_TOOLKIT_INPUT_STREAM, DumpContentType.FULL, null, null);
		mwDumpFile.prepareDumpFile();
		return mwDumpFile;
	}
	
	private Socket createSocket(String address)
	throws URISyntaxException, UnknownHostException, IOException {
		URI uri = new URI(URI_PROTOCOL_TCP + address);
		String host = uri.getHost();
		int port = uri.getPort();
		return new Socket(host, port);
	}
}
