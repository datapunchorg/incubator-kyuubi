/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.util

import java.net.{Authenticator, PasswordAuthentication, URI}
import java.net.http.{HttpClient, HttpRequest}
import java.net.http.HttpResponse.BodyHandlers
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.kyuubi.Logging

object HttpUtils extends Logging {
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def postHttp[T](url: String, requestObject: T): String = {
    postHttp[T](url, requestObject, null, null)
  }

  def postHttp[T](url: String, requestObject: T, user: String, password: String): String = {
    try {
      postHttpImpl(url, requestObject, user, password)
    } catch {
      case ex: Throwable =>
        warn(s"Failed to post to url $url", ex)
        throw ex
    }
  }

  def getHttp(url: String): String = {
    try {
      getHttpImpl(url)
    } catch {
      case ex: Throwable =>
        warn(s"Failed to get url $url", ex)
        throw ex
    }
  }

  private def postHttpImpl[T](url: String,
                              requestObject: T,
                              user: String,
                              password: String): String = {
    val props = System.getProperties()
    props.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true")
    val requestBody = objectMapper.writeValueAsString(requestObject)
    var clientBuilder = HttpClient.newBuilder()
      .sslContext(createSSLContext())
    if (user != null && !user.isEmpty) {
      clientBuilder = clientBuilder.authenticator(new Authenticator() {
        override def getPasswordAuthentication: PasswordAuthentication = {
          new PasswordAuthentication(user, password.toCharArray());
        }
      })
    }
    val client = clientBuilder.build()
    val request = HttpRequest.newBuilder()
      .uri(new URI(url))
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(requestBody))
      .build()
    info(s"Posting to url $url: $requestBody")
    val responseBody = client.send(request, BodyHandlers.ofString()).body()
    info(s"Got response from url $url: $responseBody")
    responseBody
  }

  private def getHttpImpl(url: String): String = {
    val props = System.getProperties()
    props.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true")
    val clientBuilder = HttpClient.newBuilder()
      .sslContext(createSSLContext())
    val client = clientBuilder.build()
    val request = HttpRequest.newBuilder()
      .uri(new URI(url))
      .header("Accept", "application/json")
      .GET()
      .build()
    info(s"Getting url $url")
    val responseBody = client.send(request, BodyHandlers.ofString()).body()
    info(s"Got response from url $url: $responseBody")
    responseBody
  }

  private def createSSLContext() = {
    val trustManager: TrustManager = new X509TrustManager {
      override def checkClientTrusted(
          x509Certificates: Array[X509Certificate],
          s: String): Unit = {}

      override def checkServerTrusted(
          x509Certificates: Array[X509Certificate],
          s: String): Unit = {}

      override def getAcceptedIssuers: Array[X509Certificate] = {
        null
      }
    }
    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(null, Array(trustManager), new SecureRandom())
    sslContext
  }
}
