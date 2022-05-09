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

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.client.{DiscoveryClient, ServiceDiscovery, ServiceNodeInfo}

class RestServiceDiscoveryClient(val rootUrl: String = "http://localhost:8080/api/v1")
  extends DiscoveryClient with Logging {

  private val objectMapper = new ObjectMapper()

  objectMapper.registerModule(DefaultScalaModule)

  /**
   * Create a discovery client.
   */
  override def createClient(): Unit = {
  }

  /**
   * Close the discovery client.
   */
  override def closeClient(): Unit = {
  }

  /**
   * Create path on discovery service.
   */
  override def create(path: String, mode: String, createParent: Boolean): String = {
    val url = s"$rootUrl/serviceRegistry/createPath"
    val requestObject = new CreatePathRequest(path = path,
      mode = mode, createParent = createParent)
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
    val responseObject = objectMapper.readValue(responseBody, classOf[CreatePathResponse])
    responseObject.path
  }

  /**
   * Get the stored data under path.
   */
  override def getData(path: String): Array[Byte] = {
    throw new NotImplementedError()
  }

  /**
   * Get the paths under given path.
   *
   * @return list of path
   */
  override def getChildren(path: String): List[String] = {
    throw new NotImplementedError()
  }

  /**
   * Check if the path is exists.
   */
override def pathExists(path: String): Boolean = {
    throw new NotImplementedError()
  }

  /**
   * Check if the path non exists.
   */
override def pathNonExists(path: String): Boolean = {
    throw new NotImplementedError()
  }

  /**
   * Delete a path.
   *
   * @param path           the path to be deleted
   * @param deleteChildren if true, will also delete children if they exist.
   */
override def delete(path: String, deleteChildren: Boolean): Unit = {
    throw new NotImplementedError()
  }

  /**
   * Add a monitor for serviceDiscovery. It is used to stop service discovery gracefully
   * when disconnect.
   */
override def monitorState(serviceDiscovery: ServiceDiscovery): Unit = {
    throw new NotImplementedError()
  }

  /**
   * The distributed lock path used to ensure only once engine being created for non-CONNECTION
   * share level.
   */
override def tryWithLock[T](lockPath: String, timeout: Long, unit: TimeUnit)(f: => T): T = {
    throw new NotImplementedError()
  }

  /**
   * Get the engine address and port from engine space.
   *
   * @return engine host and port
   */
  override def getServerHost(namespace: String): Option[(String, Int)] = {
    throw new NotImplementedError()
  }

  /**
   * Get engine info by engine ref id from engine space.
   *
   * @param namespace   the path to get engine ref
   * @param engineRefId engine ref id
   * @return engine host and port
   */
  override def getEngineByRefId(namespace: String, engineRefId: String): Option[(String, Int)] = {
    throw new NotImplementedError()
  }

  override def getServiceNodesInfo(namespace: String, sizeOpt: Option[Int],
                                   silent: Boolean): Seq[ServiceNodeInfo] = {
    throw new NotImplementedError()
  }

  override def registerService(conf: KyuubiConf, namespace: String,
                               serviceDiscovery: ServiceDiscovery,
                               version: Option[String], external: Boolean): Unit = {
    throw new NotImplementedError()
  }

  override def registerServiceSimple(conf: KyuubiConf, namespace: String,
                                     connectionUrl: String,
                                     version: Option[String], external: Boolean): Unit = {
    throw new NotImplementedError()
  }

  /**
   * Deregister Kyuubi instance on discovery service.
   */
  override def deregisterService(): Unit = {
    throw new NotImplementedError()
  }

  /**
   * Request remove Kyuubi instance on discovery service.
   */
  override def postDeregisterService(namespace: String): Boolean = {
    throw new NotImplementedError()
  }

  override def createAndGetServiceNode(conf: KyuubiConf, namespace: String,
                                       instance: String, version: Option[String],
                                       external: Boolean): String = {
    throw new NotImplementedError()
  }

  override def startSecretNode(createMode: String, basePath: String,
                               initData: String, useProtection: Boolean): Unit = {
    throw new NotImplementedError()
  }
}

object RestServiceDiscoveryClient extends Logging {
  def main(args: Array[String]): Unit = {
    val client = new RestServiceDiscoveryClient("http://localhost:8080/api/v1")
    val createPathResponse = client.create("/path1", "PERSISTENT")
    info(createPathResponse)
  }
}
