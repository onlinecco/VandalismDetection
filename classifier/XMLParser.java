/**
 * Created by Lunan on 12/6/2016.
 */
import javax.xml.stream.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.*;
import java.io.*;
import java.util.*;

public class XMLParser {

    public static void xmlParser(String fileName){
        try {
            FileReader fr = new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            FileWriter wr = new FileWriter("result.xml");
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
            is = new FileInputStream("result.xml");
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
                XMLEvent event = eventReader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElement = event.asStartElement();
                    System.out.println("processing element: " + startElement.getName().getLocalPart());
                    stack.push(startElement);
                }
                if(event.isEndElement()){
                    stack.pop();
                }
            }catch(XMLStreamException e) {
                FileWriter fw = null;
                try {
                    fw = new FileWriter("result.xml", true);
                } catch (IOException e2) {
                    e2.printStackTrace();
                }
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw);
                while(!stack.empty()){
                    System.out.println(stack.size());
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

    }

    public static void main(String [] args){
        xmlParser("test1.xml");
    }


}
