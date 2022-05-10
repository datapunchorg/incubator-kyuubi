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

package org.apache.kyuubi.ha.client.restclient

import java.net.{URI, URLEncoder}
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.client.{DiscoveryClient, ServiceDiscovery, ServiceNodeInfo}

class RestServiceDiscoveryClient(conf: KyuubiConf)
  extends DiscoveryClient with Logging {

  val rootUrl: String = conf.get(KyuubiConf.DISCOVERY_CLIENT_REST_URL)

  private val objectMapper = new ObjectMapper()

  objectMapper.registerModule(DefaultScalaModule)

  /**
   * Create a discovery client.
   */
  override def createClient(): Unit = {}

  /**
   * Close the discovery client.
   */
  override def closeClient(): Unit = {}

  /**
   * Create path on discovery service.
   */
  override def create(path: String, mode: String, createParent: Boolean): String = {
    val url = s"$rootUrl/serviceRegistry/createPath"
    val requestObject = new CreatePathRequest(path = path, mode = mode, createParent = createParent)
    val responseBody = postHttp(url, requestObject)
    val responseObject = objectMapper.readValue(responseBody, classOf[CreatePathResponse])
    responseObject.path
  }

  /**
   * Get the stored data under path.
   */
  override def getData(path: String): Array[Byte] = {
    val url = s"$rootUrl/serviceRegistry/getPathData?path=${encodeQueryParameter(path)}"
    val responseBody = getHttp(url)
    val responseObject = objectMapper.readValue(responseBody, classOf[GetPathDataResponse])
    responseObject.data
  }

  /**
   * Get the paths under given path.
   *
   * @return list of path
   */
  override def getChildren(path: String): List[String] = {
    val url = s"$rootUrl/serviceRegistry/getPathChildren?path=${encodeQueryParameter(path)}"
    val responseBody = getHttp(url)
    val responseObject = objectMapper.readValue(responseBody, classOf[GetPathChildrenResponse])
    responseObject.children.toList
  }

  /**
   * Check if the path is exists.
   */
  override def pathExists(path: String): Boolean = {
    val url = s"$rootUrl/serviceRegistry/getPathExists?path=${encodeQueryParameter(path)}"
    val responseBody = getHttp(url)
    val responseObject = objectMapper.readValue(responseBody, classOf[GetPathExistsResponse])
    responseObject.exists
  }

  /**
   * Check if the path non exists.
   */
  override def pathNonExists(path: String): Boolean = {
    !pathExists(path)
  }

  /**
   * Delete a path.
   *
   * @param path           the path to be deleted
   * @param deleteChildren if true, will also delete children if they exist.
   */
  override def delete(path: String, deleteChildren: Boolean): Unit = {
    val url = s"$rootUrl/serviceRegistry/deletePath?path=${encodeQueryParameter(path)}" +
      s"&deleteChildren=${deleteChildren}"
    val responseBody = getHttp(url)
    objectMapper.readValue(responseBody, classOf[DeletePathResponse])
  }

  /**
   * Add a monitor for serviceDiscovery. It is used to stop service discovery gracefully
   * when disconnect.
   */
  override def monitorState(serviceDiscovery: ServiceDiscovery): Unit = {
    warn(s"monitorState not implemented in ${this.getClass.getSimpleName}")
  }

  /**
   * The distributed lock path used to ensure only once engine being created for non-CONNECTION
   * share level.
   */
  override def tryWithLock[T](lockPath: String, timeout: Long, unit: TimeUnit)(f: => T): T = {
    warn(s"tryWithLock not implemented in ${this.getClass.getSimpleName}")
    f
  }

  /**
   * Get the engine address and port from engine space.
   *
   * @return engine host and port
   */
  override def getServerHost(namespace: String): Option[(String, Int)] = {
    val url = s"$rootUrl/serviceRegistry/getServerHost?namespace=${encodeQueryParameter(namespace)}"
    val responseBody = getHttp(url)
    val responseObject = objectMapper.readValue(responseBody, classOf[GetServerHostResponse])
    if (responseObject.host == null) {
      None
    } else {
      Some((responseObject.host, responseObject.port))
    }
  }

  /**
   * Get engine info by engine ref id from engine space.
   *
   * @param namespace   the path to get engine ref
   * @param engineRefId engine ref id
   * @return engine host and port
   */
  override def getEngineByRefId(namespace: String, engineRefId: String): Option[(String, Int)] = {
    val url = s"$rootUrl/serviceRegistry/getEngineByRefId?" +
      s"namespace=${encodeQueryParameter(namespace)}" +
      s"&engineRefId=${encodeQueryParameter(engineRefId)}"
    val responseBody = getHttp(url)
    val responseObject = objectMapper.readValue(responseBody, classOf[GetEngineByRefIdResponse])
    if (responseObject.host == null) {
      None
    } else {
      Some((responseObject.host, responseObject.port))
    }
  }

  override def getServiceNodesInfo(
      namespace: String,
      sizeOpt: Option[Int],
      silent: Boolean): Seq[ServiceNodeInfo] = {
    val url = s"$rootUrl/serviceRegistry/getServiceNodesInfo?" +
      s"namespace=${encodeQueryParameter(namespace)}" +
      s"&sizeOpt=${sizeOpt.getOrElse(0)}" +
      s"&silent=${silent}"
    val responseBody = getHttp(url)
    val responseObject = objectMapper.readValue(responseBody, classOf[GetServiceNodesInfoResponse])
    responseObject.data.map(
      GetServiceNodesInfoResponseServiceNodeInfo.convert(_)).toList
  }

  override def registerService(
      conf: KyuubiConf,
      namespace: String,
      serviceDiscovery: ServiceDiscovery,
      refId: Option[String],
      version: Option[String],
      external: Boolean): Unit = {
    registerExternalService(conf, namespace, serviceDiscovery.fe.connectionUrl, refId, version)
  }

  override def registerExternalService(
      conf: KyuubiConf,
      namespace: String,
      connectionUrl: String,
      refId: Option[String],
      version: Option[String]): Unit = {
    val url = s"$rootUrl/serviceRegistry/registerService"
    val requestObject = new RegisterServiceRequest(
      namespace = namespace,
      connectionUrl = connectionUrl,
      refId = refId.getOrElse(null),
      version = version.getOrElse(null),
      external = true)
    val responseBody = postHttp(url, requestObject)
    objectMapper.readValue(responseBody, classOf[RegisterServiceResponse])
  }

  /**
   * Deregister Kyuubi instance on discovery service.
   */
  override def deregisterService(): Unit = {
    warn(s"deregisterService not implemented in ${this.getClass.getSimpleName}")
  }

  /**
   * Request remove Kyuubi instance on discovery service.
   */
  override def postDeregisterService(namespace: String): Boolean = {
    warn(s"postDeregisterService not implemented in ${this.getClass.getSimpleName}")
    true
  }

  override def createAndGetServiceNode(
      conf: KyuubiConf,
      namespace: String,
      instance: String,
      refId: Option[String],
      version: Option[String],
      external: Boolean): String = {
    val url = s"$rootUrl/serviceRegistry/createAndGetServiceNode"
    val requestObject = new CreateAndGetServiceNodeRequest(
      namespace = namespace,
      instance = instance,
      refId = refId.getOrElse(null),
      version = version.getOrElse(null),
      external = external)
    val responseBody = postHttp(url, requestObject)
    val responseObject =
      objectMapper.readValue(responseBody, classOf[CreateAndGetServiceNodeResponse])
    responseObject.path
  }

  override def startSecretNode(
      createMode: String,
      basePath: String,
      initData: String,
      useProtection: Boolean): Unit = {
    warn(s"startSecretNode not implemented in ${this.getClass.getSimpleName}")
  }

  private def getHttp(url: String) = {
    val client = HttpClient.newHttpClient
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

  private def postHttp[T](url: String, requestObject: T) = {
    val requestBody = objectMapper.writeValueAsString(requestObject)
    val client = HttpClient.newHttpClient
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

  private def encodeQueryParameter(str: String) = {
    URLEncoder.encode(str, "UTF-8")
  }
}

object RestServiceDiscoveryClient extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new KyuubiConf()
    val path = "/path1"
    val client = new RestServiceDiscoveryClient(conf)
    val createPathResponse = client.create(path, "PERSISTENT")
    info(createPathResponse)
    val getPathDataResponse = client.getData(path)
    info(getPathDataResponse)
    val getChildrenResponse = client.getChildren(path)
    info(getChildrenResponse)
    val pathExists = client.pathExists(path)
    info(pathExists)
    client.delete(path, true)

    val namespace = "/ns1"
    client.registerExternalService(
      new KyuubiConf(),
      namespace,
      "localhost:9999",
      Some("refId1"),
      None)

    client.createAndGetServiceNode(
      new KyuubiConf(),
      namespace,
      "instance1",
      Some("ref1"),
      None,
      true)

    val serverHost = client.getServerHost(namespace)
    info(serverHost)

    val engine = client.getEngineByRefId(namespace, "localhost:9999")
    info(engine)

    val nodesInfo = client.getServiceNodesInfo(namespace, None, true)
    info(nodesInfo)
  }
}
