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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

object SparkBuilderUtils extends Logging {
  val DEFAULT_FRONTEND_THRIFT_BINARY_BIND_PORT = 5050

  def addExtraKyuubiConf(conf: KyuubiConf): Map[String, String] = {
    var allConf: Map[String, String] = conf.getAll

    {
      val key = KyuubiConf.DISCOVERY_CLIENT_CLASS.key
      val value = "org.apache.kyuubi.ha.client.restclient.RestServiceDiscoveryClient"
      allConf += (key -> value)
      info(s"Add conf to Spark: $key=$value")
    }

    {
      val key = KyuubiConf.FRONTEND_REST_BIND_PORT.key
      val value = conf.get(KyuubiConf.FRONTEND_REST_BIND_PORT).toString
      allConf += (key -> value)
      info(s"Add conf to Spark: $key=$value")
    }

    if (!allConf.contains(KyuubiConf.DISCOVERY_CLIENT_REST_URL.key)) {
      val key = KyuubiConf.DISCOVERY_CLIENT_REST_URL.key
      val value = conf.getLocalFrontendRestApiRootUrl()
      allConf += (key -> value)
      info(s"Add conf to Spark: $key=$value")
    }

    if (!allConf.contains(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT.key)) {
      val key = KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT.key
      val value = DEFAULT_FRONTEND_THRIFT_BINARY_BIND_PORT
      allConf += (key -> value.toString)
      info(s"Add conf to Spark: $key=$value")
    }

    {
      val port = allConf.getOrElse(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT.key,
        DEFAULT_FRONTEND_THRIFT_BINARY_BIND_PORT.toString)
      val key = "spark.ui.extra.ports"
      val value = port
      allConf += (key -> value)
      info(s"Add conf to Spark: $key=$value")
    }

    allConf
  }

  def generateSparkConf(conf: KyuubiConf): Map[String, String] = {
    val allConf = addExtraKyuubiConf(conf)

    var sparkConf = Map[String, String]()

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
    sparkConf
  }

}
