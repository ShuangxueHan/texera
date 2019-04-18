package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.dataflow.resource.dictionary.DictionaryManager;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.TexeraWebResponse;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import java.io.*;

@Path("/upload")
@Consumes(MediaType.MULTIPART_FORM_DATA)
@Produces(MediaType.APPLICATION_JSON)
public class FileUploadResource {
	@POST
	@Path("/dictionary")
	public TexeraWebResponse uploadDictionaryFile(
					@FormDataParam("file") InputStream uploadedInputStream,
					@FormDataParam("file") FormDataContentDisposition fileDetail) throws Exception {
		StringBuilder dictionary = new StringBuilder();

		String line = "";
		try (BufferedReader br = new BufferedReader(new InputStreamReader(uploadedInputStream))) {
			while ((line = br.readLine()) != null) {
				dictionary.append(line);
			}
		} catch (IOException e) {
			throw new TexeraWebException("Error occurred whlie uploading dictionary");
		}
		
		String fileName = fileDetail.getFileName();

		// save the dictionary
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        dictionaryManager.addDictionary(fileName, dictionary.toString());

		return new TexeraWebResponse(0, "Dictionary is uploaded");
	}


}