package com.rayful.bulk.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import com.itextpdf.text.Chunk;
import com.itextpdf.text.Document;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.PdfWriter;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.logging.RayfulLogger;

public class PdfConverter {
	/** logger 지정 */
	static Logger logger = RayfulLogger.getLogger(PdfConverter.class.getName(), Config.LOG_PATH );
	
	public static String convertPdf(String sourcFilePath) {
		// PDF to TEXT 
		String sourceFileName = sourcFilePath; 
		String extractText = null; 
		
		File source = new File(sourceFileName); 
		PDDocument pdfDoc = null;
		
		PdfWriter writer = null;
		
		try {
			pdfDoc = PDDocument.load(source);
			extractText = new PDFTextStripper().getText(pdfDoc); 

			//System.out.println(extractText);
			pdfDoc.close();
		} catch (IOException e) {
			logger.error ( e.toString() );
		} finally {try {pdfDoc.close();} catch (IOException e) {}}
		
		// TEXT to PDF
		Document document = new Document(); // pdf문서를 처리하는 객체
		String newFilePath = null; 
		try {
			newFilePath = sourcFilePath.substring(0, sourcFilePath.lastIndexOf(".")) + "_convert" +sourcFilePath.substring(sourcFilePath.lastIndexOf("."), sourcFilePath.length());
			writer = PdfWriter.getInstance(document, new FileOutputStream(newFilePath));

			document.open();
			
			BaseFont baseFont = BaseFont.createFont("malgun.ttf", BaseFont.IDENTITY_H, BaseFont.EMBEDDED);
			
			Font font = new Font(baseFont, 12);
			
			String[] lines = extractText.split(System.getProperty("line.separator"));
            
			for(int i =0; i < lines.length; i++) {
				if(lines[i].trim().length() > 0) {
					Chunk chunk = new Chunk(lines[i], font);
		            Paragraph ph = new Paragraph(chunk);
		            ph.setAlignment(Element.ALIGN_LEFT);
		            document.add(ph);
				}
	            //document.add(Chunk.NEWLINE);
			}
			            
			document.close();
			writer.close();
        } catch (Exception e1) {
        	logger.error ( e1.toString() );
        	document.close();
        	writer.close();
        } finally {
        	document.close();
        	writer.close();
        }
		
		return newFilePath;
	}

}
