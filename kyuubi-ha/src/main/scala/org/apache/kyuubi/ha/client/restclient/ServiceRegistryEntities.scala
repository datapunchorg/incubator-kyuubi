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

import com.fasterxml.jackson.annotation.JsonProperty

import org.apache.kyuubi.ha.client.ServiceNodeInfo

case class DeletePathResponse()

case class CreatePathRequest(@JsonProperty("path") path: String,
                             @JsonProperty("mode") mode: String,
                             @JsonProperty("createParent") createParent: Boolean = true)

case class CreatePathResponse(@JsonProperty("path") path: String)

case class GetPathDataResponse(@JsonProperty("data") data: Array[Byte])

case class GetPathChildrenResponse(@JsonProperty("children") children: Array[String])

case class GetPathExistsResponse(@JsonProperty("exists") exists: Boolean)

case class GetServerHostResponse(@JsonProperty("host") host: String,
                                 @JsonProperty("port") port: Int)

case class GetEngineByRefIdResponse(@JsonProperty("host") host: String,
                                    @JsonProperty("port") port: Int)

case class GetServiceNodesInfoResponse(@JsonProperty("data") data:
                                       Array[GetServiceNodesInfoResponseServiceNodeInfo])

case class GetServiceNodesInfoResponseServiceNodeInfo(
                           @JsonProperty("namespace") namespace: String,
                           @JsonProperty("nodeName") nodeName: String,
                           @JsonProperty("host") host: String,
                           @JsonProperty("port") port: Int,
                           @JsonProperty("version") version: String,
                           @JsonProperty("engineRefId") engineRefId: String) {
}

object GetServiceNodesInfoResponseServiceNodeInfo {
  def convert(info: ServiceNodeInfo):
  GetServiceNodesInfoResponseServiceNodeInfo = {
    GetServiceNodesInfoResponseServiceNodeInfo(
      namespace = info.namespace,
      nodeName = info.nodeName,
      host = info.host,
      port = info.port,
      version = info.version.getOrElse(null),
      engineRefId = info.engineRefId.getOrElse(null)
    )
  }

  def convert(info: GetServiceNodesInfoResponseServiceNodeInfo):
  ServiceNodeInfo = {
    ServiceNodeInfo(
      namespace = info.namespace,
      nodeName = info.nodeName,
      host = info.host,
      port = info.port,
      version = Option(info.version),
      engineRefId = Option(info.engineRefId)
    )
  }
}

case class RegisterServiceRequest(@JsonProperty("namespace") namespace: String,
                                  @JsonProperty("connectionUrl") connectionUrl: String,
                                  @JsonProperty("refId") refId: String,
                                  @JsonProperty("version") version: String = null,
                                  @JsonProperty("external") external: Boolean = false)

case class RegisterServiceResponse()

case class CreateAndGetServiceNodeRequest(@JsonProperty("namespace") namespace: String,
                                          @JsonProperty("instance") instance: String,
                                          @JsonProperty("refId") refId: String,
                                          @JsonProperty("version") version: String = null,
                                          @JsonProperty("external") external: Boolean = false)

case class CreateAndGetServiceNodeResponse(@JsonProperty("path") path: String)
