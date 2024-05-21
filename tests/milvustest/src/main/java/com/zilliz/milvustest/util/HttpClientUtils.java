package com.zilliz.milvustest.util;


import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @Author yongpeng.li
 * @Date 2022/9/2 11:24
 *
 * 封装了一些采用HttpClient发送HTTP请求的方法
 *
 * @see :本工具所采用的是最新的HttpComponents-Client-4.2.1
 */
public class HttpClientUtils {

    private static Log logger = LogFactory.getLog(HttpClientUtils.class);

    /**
     * 设置请求头和参数 post提交
     *
     * @param urlStr
     *            地址
     * @param headMap
     *            请求头
     * @param paramMap
     *            内容参数
     * @return
     */
    public static String connectPost(String urlStr, Map<String, String> headMap, Map<String, String> paramMap) {
        logger.info("========设置请求头和参数并以 post提交=======");
        URL url;
        String sCurrentLine = "";
        String sTotalString = "";

        DataOutputStream out = null;

        try {
            url = new URL(urlStr);
            logger.info("请求地址:" + urlStr);
            URLConnection URLconnection = url.openConnection();
            HttpURLConnection httpConnection = (HttpURLConnection) URLconnection;
            // httpConnection.setRequestProperty("Content-type", "application/json");
            httpConnection.setRequestProperty("Accept-Charset", "utf-8");
            httpConnection.setRequestProperty("contentType", "utf-8");

            if (headMap != null && !headMap.isEmpty()) {
                for (String key : headMap.keySet()) {
                    logger.info("头部信息key:" + key + "===值: " + headMap.get(key));
                    httpConnection.setRequestProperty(key, headMap.get(key));
                }
            }

            httpConnection.setRequestMethod("POST");

            httpConnection.setDoOutput(true);
            httpConnection.setDoInput(true);

            StringBuffer params = new StringBuffer();
            // 表单参数与get形式一样
            if (paramMap != null && !paramMap.isEmpty()) {
                for (String key : paramMap.keySet()) {
                    if (params.length() > 1) {
                        params.append("&");
                    }
                    params.append(key).append("=").append(paramMap.get(key).trim());

                }
                logger.info("请求参数: " + params.toString());
            }
            //System.out.println("params = " + params.toString());
            out = new DataOutputStream(httpConnection.getOutputStream());
            // 发送请求参数
            if (params!=null) {
                out.writeBytes(params.toString());
            }
            // flush输出流的缓冲
            out.flush();
            // int responseCode = httpConnection.getResponseCode();
            // if (responseCode == HttpURLConnection.HTTP_OK) {
            InputStream urlStream = httpConnection.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(urlStream));

            while ((sCurrentLine = bufferedReader.readLine()) != null) {
                sTotalString += sCurrentLine;
            }
            // //System.out.println(sTotalString);
            // 假设该url页面输出为"OK"

            // }else{
            // System.err.println("FIAL");
            // }
        } catch (Exception e) {
            logger.info("请求错误: " + e.getMessage());
            logger.error("系统错误:",e);
        } finally {

        }
        logger.info("响应信息: " + sTotalString);
        return sTotalString;
    }

    /**
     * Http Get方法
     *
     * @param url
     * @param param
     * @return
     */
    public static String doGet(String url, Map<String, String> param) {
        // 创建Httpclient对象
        CloseableHttpClient httpclient = HttpClients.createDefault();
        String resultString = "";
        CloseableHttpResponse response = null;
        try {
            // 创建uri
            URIBuilder builder = new URIBuilder(url);
            if (param != null) {
                for (String key : param.keySet()) {
                    builder.addParameter(key, param.get(key));
                }
            }
            URI uri = builder.build();

            // 创建http GET请求
            HttpGet httpGet = new HttpGet(uri);

            // 执行请求
            response = httpclient.execute(httpGet);
            // 判断返回状态是否为200
            if (response.getStatusLine().getStatusCode() == 200) {
                resultString = EntityUtils.toString(response.getEntity(), "UTF-8");
            }
        } catch (Exception e) {
            logger.error("系统错误:",e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
                httpclient.close();
            } catch (IOException e) {
                logger.error("系统错误:",e);
            }
        }
        return resultString;
    }

    /**
     * Http Get方法
     *
     * @param url
     * @param param
     * @return
     */
    public static String doGet(String url,Map<String, String> headMap,Map<String, String> param) {
        // 创建Httpclient对象
        CloseableHttpClient httpclient = HttpClients.createDefault();
        String resultString = "";
        CloseableHttpResponse response = null;
        try {
            // 创建uri
            URIBuilder builder = new URIBuilder(url);
            if (param != null) {
                for (String key : param.keySet()) {
                    builder.addParameter(key, param.get(key));
                }
            }

            URI uri = builder.build();

            // 创建http GET请求
            HttpGet httpGet = new HttpGet(uri);

            if (headMap != null && !headMap.isEmpty()) {
                for (String key : headMap.keySet()) {
                    logger.info("头部信息key:" + key + "===值: " + headMap.get(key));
                    httpGet.addHeader(key, headMap.get(key));
                }
            }

            // 执行请求
            response = httpclient.execute(httpGet);
            // 判断返回状态是否为200
            if (response.getStatusLine().getStatusCode() == 200) {
                resultString = EntityUtils.toString(response.getEntity(), "UTF-8");
            }
        } catch (Exception e) {
            logger.error("系统错误:",e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
                httpclient.close();
            } catch (IOException e) {
                logger.error("系统错误:",e);
            }
        }
        return resultString;
    }

    public static String doGet(String url) {
        return doGet(url, null);
    }


    /**
     * httpclient post方法
     *
     * @param url
     * @param param
     * @return
     */
    public static String doPost(String url,Map<String, String> headers,Map<String, String> param) {
        // 创建Httpclient对象
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        String resultString = "";
        try {
            // 创建Http Post请求
            HttpPost httpPost = new HttpPost(url);
            if(headers != null) {
                for (String key : headers.keySet()) {
                    httpPost.setHeader(key, headers.get(key));
                }
            }
            // 创建参数列表
            if (param != null) {
                /*List<NameValuePair> paramList = new ArrayList<>();
                for (String key : param.keySet()) {
                    paramList.add(new BasicNameValuePair(key, param.get(key)));
                }*/
                // 模拟表单
                //UrlEncodedFormEntity entity = new UrlEncodedFormEntity(param, "utf-8");
                HttpEntity entity1=new StringEntity(new Gson().toJson(param),"utf-8");
                httpPost.setEntity(entity1);
            }
            // 执行http请求
            response = httpClient.execute(httpPost);
            resultString = EntityUtils.toString(response.getEntity(), "utf-8");
        } catch (Exception e) {
            logger.error("系统错误:",e);
        } finally {
            try {
                if (response!=null) {
                    response.close();
                }
            } catch (IOException e) {
                logger.error("系统错误:",e);
            }
        }
        return resultString;
    }

    public static String doPost(String url) {
        return doPost(url,null,null);
    }

    /**
     * 请求的参数类型为json
     *
     * @param url
     * @param json
     * @return {username:"",pass:""}
     */
    public static String doPostJson(String url, String json) {

        logger.info("=====请求地址:"+url);
        // 创建Httpclient对象
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        String resultString = "";
        try {
            // 创建Http Post请求
            HttpPost httpPost = new HttpPost(url);
            // 创建请求内容
            logger.info("=====请求参数:"+json);
            StringEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);
            // 执行http请求
            response = httpClient.execute(httpPost);
            logger.info("=====响应参数:"+response);
            resultString = EntityUtils.toString(response.getEntity(), "utf-8");
        } catch (Exception e) {
            logger.error("系统错误:",e);
        } finally {
            try {
                if (response!=null) {
                    response.close();
                }
            } catch (IOException e) {
                logger.error("系统错误:",e);
            }
        }
        return resultString;
    }

    /**
     * 发送HTTP_GET请求
     *
     * @see -该方法会自动关闭连接,释放资源
     * @param -requestURL
     *            请求地址(含参数)
     * @param decodeCharset
     *            解码字符集,解析响应数据时用之,其为null时默认采用UTF-8解码
     * @return 远程主机响应正文
     */
    public static String sendGetRequest(String reqURL, String decodeCharset) {
        long responseLength = 0; // 响应长度
        String responseContent = null; // 响应内容
        HttpClient httpClient = new DefaultHttpClient(); // 创建默认的httpClient实例
        HttpGet httpGet = new HttpGet(reqURL); // 创建org.apache.http.client.methods.HttpGet
        try {
            HttpResponse response = httpClient.execute(httpGet); // 执行GET请求
            HttpEntity entity = response.getEntity(); // 获取响应实体
            if (null != entity) {
                responseLength = entity.getContentLength();
                responseContent = EntityUtils.toString(entity, decodeCharset == null ? "UTF-8" : decodeCharset);
                EntityUtils.consume(entity); // Consume response content
            }
            //System.out.println("请求地址: " + httpGet.getURI());
            //System.out.println("响应状态: " + response.getStatusLine());
            //System.out.println("响应长度: " + responseLength);
            //System.out.println("响应内容: " + responseContent);
        } catch (ClientProtocolException e) {
            logger.debug("该异常通常是协议错误导致,比如构造HttpGet对象时传入的协议不对(将'http'写成'htp')或者服务器端返回的内容不符合HTTP协议要求等,堆栈信息如下", e);
        } catch (ParseException e) {
            logger.debug(e.getMessage(), e);
        } catch (IOException e) {
            logger.debug("该异常通常是网络原因引起的,如HTTP服务器未启动等,堆栈信息如下", e);
        } finally {
            httpClient.getConnectionManager().shutdown(); // 关闭连接,释放资源
        }
        return responseContent;
    }

    /**
     * 发送HTTP_POST请求
     *
     * @see :该方法为<code>sendPostRequest(String,String,boolean,String,String)</code>的简化方法
     * @see :该方法在对请求数据的编码和响应数据的解码时,所采用的字符集均为UTF-8
     * @see :当<code>isEncoder=true</code>时,其会自动对<code>sendData</code>中的[中文][|][
     *      ]等特殊字符进行<code>URLEncoder.encode(string,"UTF-8")</code>
     * @param isEncoder
     *            用于指明请求数据是否需要UTF-8编码,true为需要
     */
    public static String sendPostRequest(String reqURL, String sendData, boolean isEncoder) {
        return sendPostRequest(reqURL, sendData, isEncoder, null, null);
    }

    /**
     * 发送HTTP_POST请求
     *
     * @see :该方法会自动关闭连接,释放资源
     * @see :当<code>isEncoder=true</code>时,其会自动对<code>sendData</code>中的[中文][|][
     *      ]等特殊字符进行<code>URLEncoder.encode(string,encodeCharset)</code>
     * @param reqURL
     *            请求地址
     * @param sendData
     *            请求参数,若有多个参数则应拼接成param11=value11&22=value22&33=value33的形式后,传入该参数中
     * @param isEncoder
     *            请求数据是否需要encodeCharset编码,true为需要
     * @param encodeCharset
     *            编码字符集,编码请求数据时用之,其为null时默认采用UTF-8解码
     * @param decodeCharset
     *            解码字符集,解析响应数据时用之,其为null时默认采用UTF-8解码
     * @return 远程主机响应正文
     */
    public static String sendPostRequest(String reqURL, String sendData, boolean isEncoder, String encodeCharset,
                                         String decodeCharset) {
        String responseContent = null;
        HttpClient httpClient = new DefaultHttpClient();

        HttpPost httpPost = new HttpPost(reqURL);
        // httpPost.setHeader(HTTP.CONTENT_TYPE, "application/x-www-form-urlencoded;
        // charset=UTF-8");
        httpPost.setHeader(HTTP.CONTENT_TYPE, "application/x-www-form-urlencoded");
        try {
            if (isEncoder) {
                List<NameValuePair> formParams = new ArrayList<NameValuePair>();
                for (String str : sendData.split("&")) {
                    formParams.add(new BasicNameValuePair(str.substring(0, str.indexOf("=")),
                            str.substring(str.indexOf("=") + 1)));
                }
                httpPost.setEntity(new StringEntity(
                        URLEncodedUtils.format(formParams, encodeCharset == null ? "UTF-8" : encodeCharset)));
            } else {
                httpPost.setEntity(new StringEntity(sendData));
            }

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                responseContent = EntityUtils.toString(entity, decodeCharset == null ? "UTF-8" : decodeCharset);
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            logger.debug("与[" + reqURL + "]通信过程中发生异常,堆栈信息如下", e);
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
        return responseContent;
    }

    /**
     * 发送HTTP_POST请求
     *
     * @see :该方法会自动关闭连接,释放资源
     * @see :该方法会自动对<code>params</code>中的[中文][|][
     *      ]等特殊字符进行<code>URLEncoder.encode(string,encodeCharset)</code>
     * @param reqURL
     *            请求地址
     * @param params
     *            请求参数
     * @param encodeCharset
     *            编码字符集,编码请求数据时用之,其为null时默认采用UTF-8解码
     * @param decodeCharset
     *            解码字符集,解析响应数据时用之,其为null时默认采用UTF-8解码
     * @return 远程主机响应正文
     */
    public static String sendPostRequest(String reqURL, Map<String, String> params, String encodeCharset,
                                         String decodeCharset) {
        String responseContent = null;
        HttpClient httpClient = new DefaultHttpClient();

        HttpPost httpPost = new HttpPost(reqURL);
        List<NameValuePair> formParams = new ArrayList<NameValuePair>(); // 创建参数队列
        for (Map.Entry<String, String> entry : params.entrySet()) {
            formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(formParams, encodeCharset == null ? "UTF-8" : encodeCharset));

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                responseContent = EntityUtils.toString(entity, decodeCharset == null ? "UTF-8" : decodeCharset);
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            logger.debug("与[" + reqURL + "]通信过程中发生异常,堆栈信息如下", e);
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
        return responseContent;
    }

    /**
     * 发送HTTPS_POST请求
     *
     * @see :该方法为<code>sendPostSSLRequest(String,Map<String,String>,String,String)</code>方法的简化方法
     * @see :该方法在对请求数据的编码和响应数据的解码时,所采用的字符集均为UTF-8
     * @see :该方法会自动对<code>params</code>中的[中文][|][
     *      ]等特殊字符进行<code>URLEncoder.encode(string,"UTF-8")</code>
     */
    public static String sendPostSSLRequest(String reqURL, Map<String, String> params) {
        return sendPostSSLRequest(reqURL, params, null, null);
    }

    /**
     * 发送HTTPS_POST请求
     *
     * @see :该方法会自动关闭连接,释放资源
     * @see :该方法会自动对<code>params</code>中的[中文][|][
     *      ]等特殊字符进行<code>URLEncoder.encode(string,encodeCharset)</code>
     * @param reqURL
     *            请求地址
     * @param params
     *            请求参数
     * @param encodeCharset
     *            编码字符集,编码请求数据时用之,其为null时默认采用UTF-8解码
     * @param decodeCharset
     *            解码字符集,解析响应数据时用之,其为null时默认采用UTF-8解码
     * @return 远程主机响应正文
     */
    public static String sendPostSSLRequest(String reqURL, Map<String, String> params, String encodeCharset,
                                            String decodeCharset) {
        String responseContent = "";
        HttpClient httpClient = new DefaultHttpClient();
        X509TrustManager xtm = new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            }

            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            }

            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[] { xtm }, null);
            SSLSocketFactory socketFactory = new SSLSocketFactory(ctx);
            httpClient.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 443, socketFactory));

            HttpPost httpPost = new HttpPost(reqURL);
            List<NameValuePair> formParams = new ArrayList<NameValuePair>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(formParams, encodeCharset == null ? "UTF-8" : encodeCharset));

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                responseContent = EntityUtils.toString(entity, decodeCharset == null ? "UTF-8" : decodeCharset);
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            logger.debug("与[" + reqURL + "]通信过程中发生异常,堆栈信息为", e);
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
        return responseContent;
    }

    /**
     * 发送HTTP_POST请求
     *
     * @see :若发送的<code>params</code>中含有中文,记得按照双方约定的字符集将中文<code>URLEncoder.encode(string,encodeCharset)</code>
     * @see :本方法默认的连接超时时间为30秒,默认的读取超时时间为30秒
     * @param reqURL
     *            请求地址
     * @param params
     *            发送到远程主机的正文数据,其数据类型为<code>java.util.Map<String, String></code>
     * @return 远程主机响应正文`HTTP状态码,如<code>"SUCCESS`200"</code><br>
     *         若通信过程中发生异常则返回"Failed`HTTP状态码",如<code>"Failed`500"</code>
     */
    public static String sendPostRequestByJava(String reqURL, Map<String, String> params) {
        StringBuilder sendData = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            sendData.append(entry.getKey()).append("=").append(entry.getValue()).append("&");
        }
        if (sendData.length() > 0) {
            sendData.setLength(sendData.length() - 1); // 删除最后一个&符号
        }
        return sendPostRequestByJava(reqURL, sendData.toString());
    }

    /**
     * 发送HTTP_POST请求
     *
     * @see :若发送的<code>sendData</code>中含有中文,记得按照双方约定的字符集将中文<code>URLEncoder.encode(string,encodeCharset)</code>
     * @see :本方法默认的连接超时时间为30秒,默认的读取超时时间为30秒
     * @param reqURL
     *            请求地址
     * @param sendData
     *            发送到远程主机的正文数据
     * @return 远程主机响应正文`HTTP状态码,如<code>"SUCCESS`200"</code><br>
     *         若通信过程中发生异常则返回"Failed`HTTP状态码",如<code>"Failed`500"</code>
     */
    public static String sendPostRequestByJava(String reqURL, String sendData) {
        HttpURLConnection httpURLConnection = null;
        OutputStream out = null; // 写
        InputStream in = null; // 读
        int httpStatusCode = 0; // 远程主机响应的HTTP状态码
        try {
            URL sendUrl = new URL(reqURL);
            httpURLConnection = (HttpURLConnection) sendUrl.openConnection();
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setDoOutput(true); // 指示应用程序要将数据写入URL连接,其值默认为false
            httpURLConnection.setUseCaches(false);
            httpURLConnection.setConnectTimeout(30000); // 30秒连接超时
            httpURLConnection.setReadTimeout(30000); // 30秒读取超时

            out = httpURLConnection.getOutputStream();
            out.write(sendData.toString().getBytes());

            // 清空缓冲区,发送数据
            out.flush();

            // 获取HTTP状态码
            httpStatusCode = httpURLConnection.getResponseCode();

            in = httpURLConnection.getInputStream();
            byte[] byteDatas = new byte[in.available()];
            in.read(byteDatas);
            return new String(byteDatas) + "`" + httpStatusCode;
        } catch (Exception e) {
            logger.debug(e.getMessage());
            return "Failed`" + httpStatusCode;
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    logger.debug("关闭输出流时发生异常,堆栈信息如下", e);
                }
            }
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    logger.debug("关闭输入流时发生异常,堆栈信息如下", e);
                }
            }
            if (httpURLConnection != null) {
                httpURLConnection.disconnect();
                httpURLConnection = null;
            }
        }
    }

    /**
     * https posp请求，可以绕过证书校验
     *
     * @param url
     * @param params
     * @return
     */
    public static final String sendHttpsRequestByPost(String url, Map<String, String> params) {
        String responseContent = null;
        HttpClient httpClient = new DefaultHttpClient();
        // 创建TrustManager
        X509TrustManager xtm = new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            }

            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            }

            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };
        // 这个好像是HOST验证
        X509HostnameVerifier hostnameVerifier = new X509HostnameVerifier() {
            public boolean verify(String arg0, SSLSession arg1) {
                return true;
            }

            public void verify(String arg0, SSLSocket arg1) throws IOException {
            }

            public void verify(String arg0, String[] arg1, String[] arg2) throws SSLException {
            }

            public void verify(String arg0, X509Certificate arg1) throws SSLException {
            }
        };
        try {
            // TLS1.0与SSL3.0基本上没有太大的差别，可粗略理解为TLS是SSL的继承者，但它们使用的是相同的SSLContext
            SSLContext ctx = SSLContext.getInstance("TLS");
            // 使用TrustManager来初始化该上下文，TrustManager只是被SSL的Socket所使用
            ctx.init(null, new TrustManager[] { xtm }, null);
            // 创建SSLSocketFactory
            SSLSocketFactory socketFactory = new SSLSocketFactory(ctx);
            socketFactory.setHostnameVerifier(hostnameVerifier);
            // 通过SchemeRegistry将SSLSocketFactory注册到我们的HttpClient上
            httpClient.getConnectionManager().getSchemeRegistry().register(new Scheme("https", socketFactory, 443));
            HttpPost httpPost = new HttpPost(url);
            List<NameValuePair> formParams = new ArrayList<NameValuePair>(); // 构建POST请求的表单参数
            for (Map.Entry<String, String> entry : params.entrySet()) {
                formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(formParams, "UTF-8"));
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity(); // 获取响应实体
            if (entity != null) {
                responseContent = EntityUtils.toString(entity, "UTF-8");
            }
        } catch (KeyManagementException e) {
            logger.error("系统错误:",e);
        } catch (NoSuchAlgorithmException e) {
            logger.error("系统错误:",e);
        } catch (UnsupportedEncodingException e) {
            logger.error("系统错误:",e);
        } catch (ClientProtocolException e) {
            logger.error("系统错误:",e);
        } catch (ParseException e) {
            logger.error("系统错误:",e);
        } catch (IOException e) {
            logger.error("系统错误:",e);
        } finally {
            // 关闭连接,释放资源
            httpClient.getConnectionManager().shutdown();
        }
        return responseContent;
    }

    /**
     * 发送HTTP_POST请求,json格式数据
     *
     * @param url
     * @param body
     * @return
     * @throws Exception
     */
    public static String sendPostByJson(String url, String body) throws Exception {
        CloseableHttpClient httpclient = HttpClients.custom().build();
        HttpPost post = null;
        String resData = null;
        CloseableHttpResponse result = null;
        try {
            post = new HttpPost(url);
            HttpEntity entity2 = new StringEntity(body, Consts.UTF_8);
            post.setConfig(RequestConfig.custom().setConnectTimeout(30000).setSocketTimeout(30000).build());
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Access-Token", "sund2f3bf3e7ecea902bcdb7027e9139a02");
            post.setEntity(entity2);
            result = httpclient.execute(post);
            if (HttpStatus.SC_OK == result.getStatusLine().getStatusCode()) {
                resData = EntityUtils.toString(result.getEntity());
            }
        } finally {
            if (result != null) {
                result.close();
            }
            if (post != null) {
                post.releaseConnection();
            }
            httpclient.close();
        }
        return resData;
    }

    /**
     * HttpPost发送header,Content(json格式)
     *
     * @param url
     * @param :json
     * @param headers
     * @return
     */
    public static String post(String url, Map<String, String> headers, Map<String, String> jsonMap) {

        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);
        logger.info("请求地址:" + url);
        // post.setHeader("Content-Type", "application/x-www-form-urlencoded");
        logger.info("请求头信息:" + headers);
        if (headers != null) {
            Set<String> keys = headers.keySet();
            for (Map.Entry<String, String> entrdy : headers.entrySet()) {
                post.addHeader(entrdy.getKey(), entrdy.getValue());
                //System.out.println("headers:" + entrdy.getKey() + ",值" + entrdy.getValue());
            }

        }
        String charset = null;
        try {

            StringEntity s = new StringEntity(jsonMap.toString(), "utf-8");
            logger.info("请求json参数:" + jsonMap);
            // s.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
            // s.setContentType("application/json");
            // s.setContentType(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
            post.setEntity(s);

            logger.info("请求实体数据:" + post);
            // HttpResponse res = client.execute(post);
            HttpResponse httpResponse = client.execute(post);
            InputStream inStream = httpResponse.getEntity().getContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, "utf-8"));
            StringBuilder strber = new StringBuilder();

            String line = null;
            while ((line = reader.readLine()) != null)
                strber.append(line + "\n");
            inStream.close();
            logger.info("MobilpriseActivity:" + strber);

            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = httpResponse.getEntity();
                charset = EntityUtils.getContentCharSet(entity);
            }
        } catch (Exception e) {
            logger.info("报错咯:" + e.getMessage());
            throw new RuntimeException(e);
        }
        logger.info("响应参数:" + charset);
        return charset;
    }


}

