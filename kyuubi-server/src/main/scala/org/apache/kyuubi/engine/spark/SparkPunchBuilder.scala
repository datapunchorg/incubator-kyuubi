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

import com.fasterxml.jackson.annotation.JsonProperty

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.HttpUtils

class SparkPunchBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    override val extraEngineLog: Option[OperationLog] = None,
    val restApiUrl: String) extends ProcBuilder with Logging {

  private val user = conf.get(KyuubiConf.SPARK_PUNCH_REST_API_USER)
  private val password = conf.get(KyuubiConf.SPARK_PUNCH_REST_API_PASSWORD)

  private val jarFile = conf.get(KyuubiConf.SPARK_PUNCH_SQL_ENGINE_JAR_File)
  if (jarFile == null || jarFile.isEmpty) {
    throw new KyuubiException(s"Missing value for" +
      s" ${KyuubiConf.SPARK_PUNCH_SQL_ENGINE_JAR_File.key}")
  }

  private val jarSparkVersion = conf.get(KyuubiConf.SPARK_PUNCH_SQL_ENGINE_JAR_SPARK_VERSION)
  if (jarSparkVersion == null || jarSparkVersion.isEmpty) {
    throw new KyuubiException(s"Missing value for" +
      s" ${KyuubiConf.SPARK_PUNCH_SQL_ENGINE_JAR_SPARK_VERSION.key}")
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

    val driverCores = conf.get(KyuubiConf.SPARK_PUNCH_SQL_ENGINE_DRIVER_CORES)
    val driverMemory = conf.get(KyuubiConf.SPARK_PUNCH_SQL_ENGINE_DRIVER_MEMORY)
    val executorCores = conf.get(KyuubiConf.SPARK_PUNCH_SQL_ENGINE_EXECUTOR_CORES)
    val executorMemory = conf.get(KyuubiConf.SPARK_PUNCH_SQL_ENGINE_EXECUTOR_MEMORY)
    val executorInstances = conf.get(KyuubiConf.SPARK_PUNCH_SQL_ENGINE_EXECUTOR_INSTANCES)
    val submission = PunchSparkSubmission(
      mainClass = mainClass,
      mainApplicationFile = jarFile,
      sparkVersion = jarSparkVersion,
      sparkConf = sparkConf,
      arguments = Array(),
      driver = DriverSpec(cores = driverCores, memory = driverMemory),
      executor = ExecutorSpec(cores = executorCores,
        memory = executorMemory,
        instances = executorInstances))

    val url = s"$restApiUrl/submissions"
    val responseBody = HttpUtils.postHttp(url, submission, user, password)
    info(s"Spark submission response: $responseBody")

    new SparkPunchBuilderProcess()
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

  override def destroy(): Unit = {}
}

case class PunchSparkSubmission(
    @JsonProperty("mainClass") mainClass: String,
    @JsonProperty("mainApplicationFile") mainApplicationFile: String,
    @JsonProperty("sparkVersion") sparkVersion: String,
    @JsonProperty("arguments") arguments: Array[String],
    @JsonProperty("driver") driver: DriverSpec,
    @JsonProperty("executor") executor: ExecutorSpec,
    @JsonProperty("sparkConf") sparkConf: Map[String, String])

case class DriverSpec(
    @JsonProperty("cores") val cores: Long,
    @JsonProperty("memory") val memory: String)

case class ExecutorSpec(
    @JsonProperty("cores") val cores: Long,
    @JsonProperty("memory") val memory: String,
    @JsonProperty("instances") instances: Long)
