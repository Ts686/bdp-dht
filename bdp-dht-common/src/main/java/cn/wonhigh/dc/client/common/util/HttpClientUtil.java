package cn.wonhigh.dc.client.common.util;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * httpClient工具类
 * 
 * @author user
 * @date 2017年11月12日 下午4:05:20
 * @version 0.1.0 
 * @copyright wonhigh.cn
 */
@SuppressWarnings("deprecation")
public class HttpClientUtil {
	private static PoolingHttpClientConnectionManager connMgr;
	private static RequestConfig requestConfig;
	private static final int MAX_TIMEOUT = 70000;
	private static CloseableHttpClient httpClient;
	private static CloseableHttpClient httpSSLClient;
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);
	
	/**
	 * 私有构造器
	 */
	private HttpClientUtil() {}

	static {
		// 设置连接池
		connMgr = new PoolingHttpClientConnectionManager();
		// 设置连接池大小
		connMgr.setMaxTotal(100);

		connMgr.setDefaultMaxPerRoute(connMgr.getMaxTotal());

		RequestConfig.Builder configBuilder = RequestConfig.custom();
		// 设置连接超时
		configBuilder.setConnectTimeout(MAX_TIMEOUT);
		// 设置读取超时
		configBuilder.setSocketTimeout(MAX_TIMEOUT);
		// 设置从连接池获取连接实例的超时
		configBuilder.setConnectionRequestTimeout(MAX_TIMEOUT);
		// 在提交请求之前 测试连接是否可用
		configBuilder.setStaleConnectionCheckEnabled(true);

		requestConfig = configBuilder.build();

		httpClient = HttpClients.createDefault();

		httpSSLClient = HttpClients.custom().setSSLSocketFactory(createSSLConnSocketFactory())
				.setConnectionManager(connMgr).setDefaultRequestConfig(requestConfig).build();
	}

	/**
	 * 发送 GET 请求（HTTP），不带输入数据
	 * @param url
	 * @return
	 */
	public static Map<String, Object> doGet(String url) {
		return doGet(url, new HashMap<String, Object>(1));
	}

	/**
	 * 发送 GET 请求（HTTP），K-V形式
	 * @param url
	 * @param params
	 * @return
	 */
	public static Map<String, Object> doGet(String url, Map<String, Object> params) {
		Map<String, Object> result = new HashMap<>(4);
		String apiUrl = url;
		StringBuilder param = new StringBuilder();
		int i = 0;
		for (Map.Entry<String, Object> entry : params.entrySet()) {
			if (i == 0) {
				param.append("?");
			} else {
				param.append("&");
			}
			String key = entry.getKey();
			Object value = entry.getValue();
			param.append(key).append("=").append(value);
			i++;
		}
		apiUrl += param;
		HttpGet httpPost = null;
		CloseableHttpResponse response = null;
		try {
			httpPost = new HttpGet(apiUrl);
			response = httpClient.execute(httpPost);
			int successStateCode = 200;
			if (response.getStatusLine().getStatusCode() == successStateCode) {
				String httpStr = EntityUtils.toString(response.getEntity(), "UTF-8");
				result.put("statusCode", 200);
				result.put("result", httpStr);
				return result;
			}
			result.put("statusCode", 0);
			result.put("result", "Error Response: " + response.getStatusLine().toString());

		} catch (IOException e) {
			LOGGER.error("httpGet error:", e);
			result.put("statusCode", 0);
			result.put("result", "httpGet failure :caused by-->" + e.getMessage());
		} finally {
			try {
				if (httpPost != null) {
					httpPost.releaseConnection();
				}
				if (response != null) {
					response.close();
				}
			} catch (IOException e) {
				LOGGER.error("httpGet failure :caused by-->", e);
			}
		}
		return result;
	}

	/**
	 * 发送 POST 请求（HTTP），不带输入数据
	 * @param apiUrl
	 * @return
	 */
	public static Map<String, Object> doPost(String apiUrl) {
		return doPost(apiUrl, new HashMap<String, String>(1));
	}

	/**
	 * 发送 POST 请求（HTTP），K-V形式
	 * @param apiUrl API接口URL
	 * @param params 参数map
	 * @return
	 */
	public static Map<String, Object> doPost(String apiUrl, Map<String, String> params) {
		Map<String, Object> result = new HashMap<>(3);
		String httpStr = null;
		HttpPost httpPost = null;
		CloseableHttpResponse response = null;
		try {
			httpPost = new HttpPost(apiUrl);
			httpPost.setConfig(requestConfig);
			List<NameValuePair> pairList = new ArrayList<>(params.size());
			for (Map.Entry<String, String> entry : params.entrySet()) {
				NameValuePair pair = new BasicNameValuePair(entry.getKey(), entry.getValue());
				pairList.add(pair);
			}
			httpPost.setEntity(new UrlEncodedFormEntity(pairList, Charset.forName("UTF-8")));
			response = httpSSLClient.execute(httpPost);
			int successStateCode = 200;
			if (response.getStatusLine().getStatusCode() == successStateCode) {
				httpStr = EntityUtils.toString(response.getEntity(), "UTF-8");
				result.put("statusCode", 200);
				result.put("result", httpStr);
				return result;
			}
			result.put("statusCode", 0);
			result.put("result", "Error Response: " + response.getStatusLine().toString());
		} catch (IOException e) {
			LOGGER.error("dopost error", e);
			result.put("statusCode", 0);
			result.put("result", "post failure :caused by-->" + e.getMessage());
		} finally {
			try {
				if (httpPost != null) {
					httpPost.releaseConnection();
				}
				if (response != null) {
					response.close();
				}
			} catch (IOException e) {
				LOGGER.error("httpPost failure :caused by-->", e);
			}
		}
		return result;
	}

	/**
	 * 发送 POST 请求（HTTP），JSON形式
	 * @param apiUrl
	 * @param json json对象
	 * @return
	 */
	public static Map<String, Object> doPostJson(String apiUrl, Object json) {
		Map<String, Object> result = new HashMap<>(3);
		String httpStr = null;
		HttpPost httpPost = null;
		CloseableHttpResponse response = null;
		try {
			httpPost = new HttpPost(apiUrl);
			httpPost.setConfig(requestConfig);
			// 解决中文乱码问题
			StringEntity stringEntity = new StringEntity(json.toString(), "UTF-8");
			stringEntity.setContentEncoding("UTF-8");
			stringEntity.setContentType("application/json");
			httpPost.setEntity(stringEntity);
			response = httpClient.execute(httpPost);
			HttpEntity entity = response.getEntity();
			int successStateCode = 200;
			if (response.getStatusLine().getStatusCode() == successStateCode) {
				httpStr = EntityUtils.toString(entity, "UTF-8");
				result.put("statusCode", 200);
				result.put("result", httpStr);
				return result;
			} else {
				result.put("statusCode", 0);
				result.put("result", "Error Response: " + response.getStatusLine().toString());
				return result;
			}
		} catch (IOException e) {
			LOGGER.error("doPostJson error:", e);
			result.put("statusCode", 0);
			result.put("result", "post failure :caused by-->" + e.getMessage());
			return result;
		} finally {
			try {
				if (httpPost != null) {
					httpPost.releaseConnection();
				}
				if (response != null) {
					response.close();
				}
			} catch (IOException e) {
				LOGGER.error("httpPost failure :caused by-->", e);
			}
		}
	}

	/**
	 * 发送 SSL POST 请求（HTTPS），K-V形式
	 * @param apiUrl  API接口URL
	 * @param params 参数map
	 * @return
	 */
	public static Map<String, Object> doPostSSLByMapParams(String apiUrl, Map<String, String> params) {
		Map<String, Object> result = new HashMap<>(4);
		HttpPost httpPost = null;
		CloseableHttpResponse response = null;
		try {
			httpPost = new HttpPost(apiUrl);
			httpPost.setConfig(requestConfig);
			List<NameValuePair> pairList = new ArrayList<>(params.size());
			for (Map.Entry<String, String> entry : params.entrySet()) {
				NameValuePair pair = new BasicNameValuePair(entry.getKey(), entry.getValue());
				pairList.add(pair);
			}
			httpPost.setEntity(new UrlEncodedFormEntity(pairList, Charset.forName("utf-8")));
			response = httpSSLClient.execute(httpPost);
			int statusCode = response.getStatusLine().getStatusCode();
			int successStateCode = 200;
			if (statusCode == successStateCode) {
				String srtResult = EntityUtils.toString(response.getEntity(), "utf-8");
				result.put("statusCode", 200);
				result.put("result", srtResult);
				return result;
			}
			result.put("statusCode", 0);
			result.put("result", "Error Response: " + response.getStatusLine().toString());
		} catch (IOException e) {
			LOGGER.error("doPostSSLByMapParams error:", e);
			result.put("statusCode", 0);
			result.put("result", "post failure :caused by-->" + e.getMessage());
			return result;
		} finally {
			try {
				if (httpPost != null) {
					httpPost.releaseConnection();
				}
				if (response != null) {
					response.close();
				}
			} catch (IOException e) {
				LOGGER.error("httpPost failure :caused by-->", e);
			}
		}
		return result;
	}

	/**
	 * 发送 SSL POST 请求（HTTPS），JSON形式
	 * @param apiUrl  API接口URL
	 * @param json  JSON对象
	 * @return
	 */
	public static Map<String, Object> doPostSSL(String apiUrl, Object json) {
		Map<String, Object> result = new HashMap<>(3);
		HttpPost httpPost = null;
		CloseableHttpResponse response = null;
		try {
			httpPost = new HttpPost(apiUrl);
			httpPost.setConfig(requestConfig);
			// 解决中文乱码问题
			StringEntity stringEntity = new StringEntity(json.toString(), "UTF-8");
			stringEntity.setContentEncoding("UTF-8");
			stringEntity.setContentType("application/json");
			httpPost.setEntity(stringEntity);
			response = httpSSLClient.execute(httpPost);
			int statusCode = response.getStatusLine().getStatusCode();
			int successStateCode = 200;
			if (statusCode == successStateCode) {
				String srtResult = EntityUtils.toString(response.getEntity(), "utf-8");
				result.put("statusCode", 200);
				result.put("result", srtResult);
				return result;
			}
			result.put("statusCode", 0);
			result.put("result", "Error Response: " + response.getStatusLine().toString());
		} catch (IOException e) {
			LOGGER.error("doPostSSL error", e);
		} finally {
			try {
				if (httpPost != null) {
					httpPost.releaseConnection();
				}
				if (response != null) {
					response.close();
				}
			} catch (IOException e) {
				LOGGER.error("httpPost failure :caused by-->", e);
			}
		}
		return result;
	}

	/**
	 * 创建SSL安全连接
	 * @return
	 */
	private static SSLConnectionSocketFactory createSSLConnSocketFactory() {
		SSLConnectionSocketFactory sslsf = null;
		try {
			SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
				@Override
				public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
					return true;
				}
			}).build();
			sslsf = new SSLConnectionSocketFactory(sslContext, new X509HostnameVerifier() {
				@Override
				public boolean verify(String arg0, SSLSession arg1) {
					return true;
				}

				@Override
				public void verify(String host, SSLSocket ssl) throws IOException {
				}

				@Override
				public void verify(String host, X509Certificate cert) throws SSLException {
				}

				@Override
				public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
				}
			});
		} catch (GeneralSecurityException e) {
			LOGGER.error("createSSLConnSocketFactory error:", e);
		}
		return sslsf;
	}

}
