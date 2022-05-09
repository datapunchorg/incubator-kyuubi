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

case class EmptyResponse()

case class CreatePathRequest(@JsonProperty("path") path: String,
                             @JsonProperty("mode") mode: String,
                             @JsonProperty("createParent") createParent: Boolean = true)

case class CreatePathResponse(@JsonProperty("path") path: String)

case class GetPathDataResponse(data: Array[Byte])

case class GetPathChildrenResponse(children: Array[String])

case class GetPathExistsResponse(exists: Boolean)

case class GetServerHostResponse(host: String, port: Int)

case class GetEngineByRefIdResponse(host: String, port: Int)

case class GetServiceNodesInfoResponse(data: Array[ServiceNodeInfo])

case class RegisterServiceRequest(namespace: String,
                                        connectionUrl: String,
                                        version: String = null,
                                        external: Boolean = false)

case class RegisterServiceResponse()

case class CreateAndGetServiceNodeRequest(namespace: String,
                                          instance: String,
                                  version: String = null,
                                  external: Boolean = false)

case class CreateAndGetServiceNodeResponse(path: String)
