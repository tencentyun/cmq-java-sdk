package com.qcloud.cmq;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

public class CMQHttp {
	protected static int timeout = 10000;
	protected static boolean isKeepAlive = true;
	
	public static String request(String method, String url, String req,
			int userTimeout) throws Exception {
		String result = "";
		BufferedReader in = null;
		
		try{
			URL realUrl = new URL(url);
			URLConnection connection = null;
			if (url.toLowerCase().startsWith("https")) {
				HttpsURLConnection httpsConn = (HttpsURLConnection)realUrl.openConnection();
				httpsConn.setHostnameVerifier(new HostnameVerifier() {
					public boolean verify(String hostname, SSLSession session) {
						return true;
					}
				});
				connection = httpsConn;
			} else {
				connection = realUrl.openConnection();
			}
	
			connection.setRequestProperty("Accept", "*/*");
			if(isKeepAlive)
				connection.setRequestProperty("Connection", "Keep-Alive");
			connection.setRequestProperty("User-Agent",
					"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
	
			connection.setConnectTimeout(timeout+userTimeout);
	
			if (method.equals("POST")) {
				((HttpURLConnection)connection).setRequestMethod("POST");
	
				connection.setDoOutput(true);
				connection.setDoInput(true);
				DataOutputStream out = new DataOutputStream(connection.getOutputStream());
				out.writeBytes(req);
				out.flush();
				out.close();
			}
	
	
			connection.connect();
			int status = ((HttpURLConnection)connection).getResponseCode();
			if(status != 200)
				throw new CMQServerException(status);
	
			in = new BufferedReader(new InputStreamReader(connection.getInputStream(),"utf-8"));
	
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		}catch(Exception e){
			throw e;
		}finally{
			try {
				if (in != null) 
					in.close();
			} catch (Exception e2) {
				throw e2;
			}
		}
		
		return result;
	}
}
