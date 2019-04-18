package edu.uci.ics.texera.dataflow.source.file;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextShape;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import edu.uci.ics.texera.api.exception.DataflowException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import java.io.InputStream;
import java.io.FileInputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Created by junm5 on 5/3/17.
 */
public class FileExtractorUtils {


    /**
     * Extracts data as plain text file.
     *
     * @param path
     * @return
     * @throws DataflowException
     */

    public static String extractPlainTextFile(Path path) throws DataflowException {
        try {
            return new String(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new DataflowException(e);
        }
    }

    public static List<String> readTextFileLines(Path path) {
        try {
            List<String> lines = Files.readAllLines(path);
            return lines;
        } catch (IOException e) {
            throw new DataflowException(e);
        }
    }
    public static BufferedReader readFileG(Path path){
        BufferedReader bufferedReader = null;
        try {
            FileInputStream file = new FileInputStream(path.toString());
            bufferedReader = new BufferedReader(new InputStreamReader(file));
        }catch ( FileNotFoundException  e){
            e.printStackTrace();
        }
        return  bufferedReader;
    }


    public static String extraTextFileOneLineMillion(BufferedReader bufferedReader){


            //List<String> lines = new ArrayList<>();
        String line = null;
            //Stream<String> linesStream = bufferedReader.lines();
            try{
            line = bufferedReader.readLine();

            }catch (IOException e){
                e.printStackTrace();
            }finally {

            }
            return  line;

    }

    public static List<String> readTextFileWarmLines(Path path, int lineCounter) {

        try {
            List<String> contentList = new ArrayList<>();
            List<String> lines = Files.readAllLines(path);
            String warmLines = new String();
            int i = 0;
            while (lineCounter < lines.size() && i < lineCounter) {
                //System.out.println(i + " : " +lines.get(0));
                warmLines = warmLines + lines.get(0);
                //System.out.println(" : " +warmLines);
                lines.remove(0);
                i++;
            }
            //System.out.println(" : " +warmLines);
            contentList.add(warmLines);
            contentList.addAll(lines);

            return contentList;
        } catch (IOException e) {
            throw new DataflowException(e);
        }
    }

    public static String extractPlainTextFileOneLine(List<String> lines) throws DataflowException {
        if (lines.size() > 0) {
            String line = lines.get(0);
            lines.remove(0);
            return line;
        } else
            return null;

    }

    /**
     * Extracts data from PDF document using pdfbox.
     *
     * @param path
     * @return
     * @throws DataflowException
     */
    public static String extractPDFFile(Path path) throws DataflowException {
        try (PDDocument doc = PDDocument.load(new File(path.toString()))) {
            return new PDFTextStripper().getText(doc);
        } catch (IOException e) {
            throw new DataflowException(e);
        }
    }

    /**
     * Extracts data from PPT/PPTX from using poi.
     *
     * @param path
     * @return
     * @throws DataflowException
     */
    public static String extractPPTFile(Path path) throws DataflowException {
        try (FileInputStream inputStream = new FileInputStream(path.toString());
             XMLSlideShow ppt = new XMLSlideShow(inputStream)) {
            StringBuffer res = new StringBuffer();
            for (XSLFSlide slide : ppt.getSlides()) {
                List<XSLFShape> shapes = slide.getShapes();
                for (XSLFShape shape : shapes) {
                    if (shape instanceof XSLFTextShape) {
                        XSLFTextShape textShape = (XSLFTextShape) shape;
                        String text = textShape.getText();
                        res.append(text);
                    }
                }
            }
            return res.toString();
        } catch (IOException e) {
            throw new DataflowException(e);
        }
    }

    /**
     * Extract data from MS Word DOC/DOCX file to text
     *
     * @param path
     * @return
     * @throws DataflowException
     */
    public static String extractWordFile(Path path) throws DataflowException {
        try (FileInputStream inputStream = new FileInputStream(path.toString())) {
            BodyContentHandler handler = new BodyContentHandler();
            Metadata metadata = new Metadata();

            AutoDetectParser parser = new AutoDetectParser();
            parser.parse(inputStream, handler, metadata);

            return handler.toString();
        } catch (IOException | SAXException | TikaException e) {
            throw new DataflowException(e);
        }
    }




}
