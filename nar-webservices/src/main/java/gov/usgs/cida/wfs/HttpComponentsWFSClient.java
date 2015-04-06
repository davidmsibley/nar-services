package gov.usgs.cida.wfs;

import gov.usgs.cida.gml.EmptyFeatureCollectionException;
import gov.usgs.cida.gml.GMLStreamingFeatureCollection;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.UUID;

import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.filter.FilterTransformer;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This implementation uses regular HTTP client to download wfs to temporary
 * file and builds feature collection from that.
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class HttpComponentsWFSClient implements WFSClientInterface {
	private static final Logger LOG = LoggerFactory.getLogger(HttpComponentsWFSClient.class);
    
    private String wfsEndpoint;
    private File tmpWfsFile;
    
    public HttpComponentsWFSClient() {
        this.wfsEndpoint = null;
        UUID randomUUID = UUID.randomUUID();
        this.tmpWfsFile = FileUtils.getFile(FileUtils.getTempDirectory(), randomUUID.toString() + ".xml");
    }

    @Override
    public void setupDatastoreFromGetCaps(String getCapsUrlString) throws MalformedURLException, IOException {
        throw new UnsupportedOperationException("Not supported yet. Only in Geotools version");
    }

    @Override
    public void setupDatastoreFromEndpoint(String wfsUrl) throws MalformedURLException, IOException {
        wfsEndpoint = wfsUrl;
    }

    @Override
    public SimpleFeatureCollection getFeatureCollection(String typeName, Filter filter) throws IOException {
        SimpleFeatureCollection collection= null;
        
        String filterXml = null;
        InputStream is = null;
        OutputStream os = null;
        DefaultHttpClient httpClient = new DefaultHttpClient();
        if (filter != null) {
            try {
                FilterTransformer transformer = new FilterTransformer();
                transformer.setOmitXMLDeclaration(true);
                transformer.setNamespaceDeclarationEnabled(false);
                filterXml = "<ogc:Filter>" + transformer.transform(filter) + "</ogc:Filter>";
            } catch (TransformerException ex) {
                throw new RuntimeException("Specified filter cannot be transformed", ex);
            }
        } else {
            filterXml = "";
        }
        
        try {
            HttpPost post = new HttpPost(wfsEndpoint);
            StringEntity stringEntity = new StringEntity(fillInTemplate(typeName, filterXml), ContentType.APPLICATION_XML);
            post.setEntity(stringEntity);
            HttpContext localContext = new BasicHttpContext();
            httpClient.setReuseStrategy(new NoConnectionReuseStrategy());
            HttpResponse methodResponse = httpClient.execute(post, localContext);
            if (methodResponse.getStatusLine().getStatusCode() != 200) {
                throw new IOException(methodResponse.getStatusLine().getReasonPhrase());
            }
            is = methodResponse.getEntity().getContent();
            os = new FileOutputStream(tmpWfsFile);
            IOUtils.copy(is, os);
        } finally {
            if (httpClient.getConnectionManager() != null) {
                httpClient.getConnectionManager().closeExpiredConnections();
            }
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(os);
        }
        try {
			collection = new GMLStreamingFeatureCollection(tmpWfsFile);
		} catch (EmptyFeatureCollectionException e) {
			LOG.debug("no features returned", e);
		}
        
        return collection;
    }

    @Override
    public String[] getTypeNames() throws IOException {
        throw new UnsupportedOperationException("Not supported yet. Skipping getCaps for now");
    }
    
    @Override
    public void close() {
        FileUtils.deleteQuietly(tmpWfsFile);
    }
    
    private String fillInTemplate(String typeName, String filter) {
        return String.format(wfsXMLTemplate, typeName, filter);
    }

    // I'm a bit embarrassed of this so I put it on the bottom
    // also may need to create special namespace for GetFeature request
    private static final String wfsXMLTemplate = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
"<wfs:GetFeature service=\"WFS\" outputFormat=\"text/xml; subtype=gml/3.1.1\" version=\"1.1.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:wfs=\"http://www.opengis.net/wfs\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:gml=\"http://www.opengis.net/gml\" xmlns:ogc=\"http://www.opengis.net/ogc\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xsi:schemaLocation=\"http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd\">" +
"   <wfs:Query typeName=\"%s\" srsName=\"EPSG:4326\">" +
"       %s" +
"   </wfs:Query>" +
"</wfs:GetFeature>";
    
}
