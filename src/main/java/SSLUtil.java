import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class SSLUtil {
    public static SSLContext createSSLContext(String keystorePassword, String truststorePassword) {
        // load the keystore which stores the private key and public cert
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(System.getProperty("user.dir") + "/certs/server/server-keystore.p12")) {
                keyStore.load(fis, keystorePassword.toCharArray());
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(keyStore, keystorePassword.toCharArray());
            // Load Truststore (CA cert)
            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(System.getProperty("user.dir") + "/certs/ca/ca-truststore.p12")) {
                trustStore.load(fis, truststorePassword.toCharArray());
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(trustStore);
    
            // Create SSL Context
            SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null); // create SSL context to perform the handshake using key and trust managers
            return sslContext;
        } catch (Exception e) {
            System.err.println("Error creating SSL Context for handshake: " + e.getMessage());
        }
        return null;


        // the idea is that the KeyManagers are responsible with providing the certificate and proving the identity of a resource during TLS handshakes
        // When Redis accepts a connection it presents its cert via the KeyManager and when Gateway client connects it presents its cert via the keyManager
        // The KeyManager contains the private key to digitally sign things and also the public cert to prove ownership
        // Finally the TrustManagers are responsible for deciding which certificates we will accept from others
    }
}