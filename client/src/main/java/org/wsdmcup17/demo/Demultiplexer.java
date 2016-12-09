package org.wsdmcup17.demo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

/**
 * Thread to demultiplex revisions and meta data. The resulting revisions are
 * provided as an {@link OutputStream} that can, for example, be processed with
 * Wikidata Toolkit. The metadata is parsed and put in a queue for further
 * processing.
 */
static String curr_revision_id;
public class Demultiplexer implements Runnable {
	
	private static final Logger
		LOG = Logger.getLogger(Demultiplexer.class);
	
	private static final String
		LOG_MSG_END_OF_ITEM_STREAM = "End of item stream.",
		LOG_MSG_MISSING_DATA_AFTER_METADATA = "Missing data after metadata %s";
	
	private DataInputStream dataStream;
	private CSVPrinter resultPrinter;
	private boolean firstRow;
	public Demultiplexer(
		DataInputStream inputStream,CSVPrinter resultPrinter
	) {
		this.dataStream = inputStream;
		this.resultPrinter = resultPrinter;
		firstRow = true;
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
    public void xmlParser(String fileName){
        try {
            FileReader fr = new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            FileWriter wr = new FileWriter("/home/barberry/data/temp");
            BufferedWriter bw = new BufferedWriter(wr);

            String line = null;
            while((line = br.readLine()) != null){
            	
                if(!line.contains("</"))
                    break;
            }

            bw.write(line+"\n");
            while((line = br.readLine()) != null){
                bw.write(line +"\n");
            }
            br.close();
            fr.close();
            bw.close();
            wr.close();

        } catch (FileNotFoundException e ) {
            e.printStackTrace();
        } catch (IOException ex){
            ex.printStackTrace();
        }
        
        InputStream is = null;


        try {
            is = new FileInputStream("/home/barberry/data/temp");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();

        XMLEventReader eventReader = null;


        try {
            eventReader = inputFactory.createXMLEventReader(is, "utf-8");
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        Stack<StartElement> stack = new Stack<StartElement>();
        while (eventReader.hasNext()) {
            try {
            	XMLEvent event;
            	try{
            		event = eventReader.nextEvent();
            	}catch(NullPointerException e){
            		break;
            	}
                if (event.isStartElement()) {
                    StartElement startElement = event.asStartElement();
                    //System.out.println("processing element: " + startElement.getName().getLocalPart());
                    stack.push(startElement);
                }
                if(event.isEndElement()){
                    stack.pop();
                }
            }catch(XMLStreamException e) {
                FileWriter fw = null;
                try {
                    fw = new FileWriter("/home/barberry/data/temp", true);
                } catch (IOException e2) {
                    e2.printStackTrace();
                }
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw);
                while(!stack.empty()){
                    //System.out.println(stack.size());
                    StartElement se = stack.pop();
                    String tagName = se.getName().getLocalPart();
                    out.println("</" + tagName + ">");
                }

                out.close();
                try {
                    bw.close();
                    fw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        /*
		try {
			FileReader fr = new FileReader("/home/barberry/data/temp");
			BufferedReader br = new BufferedReader(fr);
			String line = "";

			while((line = br.readLine()) != null){
				LOG.info("xxx " + line);
            }
			
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
        
    }

	private void demultiplexStream() throws IOException, InterruptedException {
		LOG.info("Thread started");
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
		LOG.info("Model Loaded");
		pipe.close();
		try {
			while (true) {

				LOG.info("reading metadata");
				// Read metadata from stream and queue it.
				byte[] bytes1 = readNextItem(dataStream);
				if (bytes1 == null) { // end of stream
					LOG.error("No metadata");
					break;
				}

				LOG.info("reading revision");
				// Read corresponding revision from stream and forward it.
				byte[] bytes2 = readNextItem(dataStream);
				if (bytes2 == null) { // end of stream
					LOG.error("No revision");
					break;
				}
				
				
				LOG.info("Writing revision and metadata...");
				File revisionFile = new File("/home/barberry/data/revision");
				
				FileOutputStream out = new FileOutputStream("/home/barberry/data/revision");
				out.write(bytes2);
				out.close();
				xmlParser("/home/barberry/data/revision");
				BZip2CompressorOutputStream om = new BZip2CompressorOutputStream(new FileOutputStream(revisionFile));
				File temp = new File("/home/barberry/data/temp");
				byte[] reorg = Files.readAllBytes(Paths.get(temp.toURI()));
				if(!firstRow){
					om.write("<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.10/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.10/ http://www.mediawiki.org/xml/export-0.10.xsd\" version=\"0.10\" xml:lang=\"en\">\n  <siteinfo>\n    <sitename>Wikidata</sitename>\n    <dbname>wikidatawiki</dbname>\n    <base>http://www.wikidata.org/wiki/Wikidata:Main_Page</base>\n    <generator/>\n    <case>first-letter</case>\n    <namespaces>\n      <namespace key=\"0\" case=\"first-letter\"/>\n    </namespaces>\n  </siteinfo>".getBytes());
				}
				om.write(reorg);
				if(!firstRow){
					om.write("</mediawiki>".getBytes());
				}
				LOG.info(new String(reorg, StandardCharsets.UTF_8));
				om.flush();
				om.close();
				
				File metadataFile = new File("/home/barberry/data/metadata");
				BZip2CompressorOutputStream om2 = new BZip2CompressorOutputStream(new FileOutputStream(metadataFile));
				if(!firstRow){
					om2.write("REVISION_ID,REVISION_SESSION_ID,USER_COUNTRY_CODE,USER_CONTINENT_CODE,USER_TIME_ZONE,USER_REGION_CODE,USER_CITY_NAME,USER_COUNTY_NAME,REVISION_TAGS\n".getBytes());
				}
				om2.write(bytes1);
				if (firstRow){
					firstRow = false;
				}
                
                try {
                    curr_revision_id = new String(bytes1).split(",")[0];
                }              
                catch { 
                    LOG.info("Revision id from meta data failed"); 
                }
                
				LOG.info(new String(bytes1, StandardCharsets.UTF_8));
				om2.flush();
				om2.close();

				
				classifyRevision(resultPrinter);
			}
		}
		finally {
			

		}
		LOG.info("closing resultPrinter");
		resultPrinter.close();
	}
	
	private float classifyRevision(CSVPrinter resultPrinter) {
		// This is where an actual classification based on  the revision and
		// its associated metadata should happen. Instead, we just assign a
		// score of 0.0, effectively classifying the revision as non-vandalism.
		Process proc;
		try {
			LOG.info("Extracting Features...");
			proc = Runtime
					.getRuntime()
					.exec("java -jar /home/barberry/programs/featureextractor.jar --revisiontags /home/barberry/data/metadata /home/barberry/data/revision /home/barberry/data/feature");
			final BufferedReader bre = new BufferedReader
			        (new InputStreamReader(proc.getInputStream()));

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
			 
			proc.waitFor();

			RandomAccessFile pipe = new RandomAccessFile(
					"/home/barberry/data/notify", "rw");
			String req = "datahasarrived";
			// Write request to the pipe
			pipe.write(req.getBytes());
			pipe.close();
			pipe = new RandomAccessFile("/home/barberry/data/notify", "r");
			// Read response from pipe
			LOG.info("Waiting for python response...");
			String res = pipe.readLine();
			pipe.close();
			if (res != null && !res.equals("")) {
				LOG.info("Retreiving results...");
				BufferedReader br = new BufferedReader(new FileReader(
						"/home/barberry/data/result"));
				try {

					String line = br.readLine();
					while (line != null) {
						String[] content = line.split(" ");
                        if (content.length() == 1) {
                            sendClassificationResult(Long.parseLong(curr_revision_id), 0, resultPrinter);
                        }
                        else {
						sendClassificationResult(Long.parseLong(content[0]),
								Float.valueOf(content[1]), resultPrinter);
                        }
						line = br.readLine();
					}

				} finally {
					br.close();
				}
			}

		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error("sha", e);
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
