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

package org.apache.kyuubi.engine.spark

import java.io.{InputStream, OutputStream}
import java.net.{Authenticator, PasswordAuthentication, URI}
import java.net.http.{HttpClient, HttpRequest}
import java.net.http.HttpResponse.BodyHandlers
import java.security.SecureRandom
import java.security.cert.X509Certificate

import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.operation.log.OperationLog

class SparkPunchBuilder(override val proxyUser: String,
                        override val conf: KyuubiConf,
                        override val extraEngineLog: Option[OperationLog] = None,
                        val restApiUrl: String
                       ) extends ProcBuilder with Logging {
  private val objectMapper = new ObjectMapper()

  objectMapper.registerModule(DefaultScalaModule)

  private val jarFile = conf.get(KyuubiConf.SPARK_PUNCH_SQL_ENGINE_JAR_File)
  if (jarFile == null || jarFile.isEmpty) {
    throw new KyuubiException(s"Missing value for" +
      s" ${KyuubiConf.SPARK_PUNCH_SQL_ENGINE_JAR_File.key}")
  }

  /**
   * The short name of the engine process builder, we use this for form the engine jar paths now
   * see `mainResource`
   */
  override def shortName: String = "spark"

  override protected def module: String = "kyuubi-spark-sql-engine"

  /**
   * The class containing the main method
   */
  override protected def mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

  override protected def commands: Array[String] = Array()

  override def clusterManager(): Option[String] = Some("PUNCH_REST_API")

  override def start: Process = synchronized {
    var sparkConf = Map[String, String]()

    var allConf = conf.getAll
    /**
     * Converts kyuubi configs to configs that Spark could identify.
     * - If the key is start with `spark.`, keep it AS IS as it is a Spark Conf
     * - If the key is start with `hadoop.`, it will be prefixed with `spark.hadoop.`
     * - Otherwise, the key will be added a `spark.` prefix
     */
    allConf.foreach { case (k, v) =>
      val newKey =
        if (k.startsWith("spark.")) {
          k
        } else if (k.startsWith("hadoop.")) {
          "spark.hadoop." + k
        } else {
          "spark." + k
        }
      sparkConf += (newKey -> v)
    }

    val submission = PunchSparkSubmission(
      mainClass = mainClass,
      mainApplicationFile = jarFile,
      sparkConf = sparkConf,
      arguments = Array(),
      driver = DriverSpec(cores = 1, memory = "1g"),
      executor = ExecutorSpec(cores = 1L, memory = "1", instances = 3)
    )

    val url = s"$restApiUrl/submissions"
    val responseBody = postHttp(url, submission)
    info(s"Submission response: $responseBody")

    new SparkPunchBuilderProcess()
  }

  private def postHttp[T](url: String, requestObject: T) = {
    val props = System.getProperties()
    props.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true")
    val trustManager: TrustManager = new X509TrustManager {
      override def checkClientTrusted(x509Certificates: Array[X509Certificate],
                                      s: String): Unit = {
      }
      override def checkServerTrusted(x509Certificates: Array[X509Certificate],
                                      s: String): Unit = {
      }
      override def getAcceptedIssuers: Array[X509Certificate] = {
        null
      }
    }
    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(null, Array(trustManager), new SecureRandom())

    val requestBody = objectMapper.writeValueAsString(requestObject)
    val client = HttpClient.newBuilder()
      .sslContext(sslContext)
      .authenticator(new Authenticator() {
        override def getPasswordAuthentication: PasswordAuthentication = {
          new PasswordAuthentication("user1", "password1".toCharArray());
        }
      })
      .build()
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
}

class SparkPunchBuilderProcess extends Process {
  override def getOutputStream: OutputStream = {
    OutputStream.nullOutputStream()
  }

  override def getInputStream: InputStream = {
    InputStream.nullInputStream()
  }

  override def getErrorStream: InputStream = {
    InputStream.nullInputStream()
  }

  override def waitFor(): Int = {
    0
  }

  override def exitValue(): Int = {
    0
  }

  override def destroy(): Unit = {
  }
}

case class PunchSparkSubmission(@JsonProperty("mainClass") mainClass: String,
                                @JsonProperty("mainApplicationFile") mainApplicationFile: String,
                                @JsonProperty("sparkVersion") sparkVersion: String = "3.2",
                                @JsonProperty("arguments") arguments: Array[String],
                                @JsonProperty("driver") driver: DriverSpec,
                                @JsonProperty("executor") executor: ExecutorSpec,
                                @JsonProperty("sparkConf") sparkConf: Map[String, String])


case class DriverSpec(@JsonProperty("cores") val cores: Long,
                      @JsonProperty("memory") val memory: String)

case class ExecutorSpec(@JsonProperty("cores") val cores: Long,
                        @JsonProperty("memory") val memory: String,
                        @JsonProperty("instances") instances: Long)
