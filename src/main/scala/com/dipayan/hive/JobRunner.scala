package com.dipayan.hive

import java.util.concurrent.Executors

import com.ning.http.client.AsyncHttpClientConfigBean
import play.api.libs.json.Json
import play.api.libs.ws.ning.NingWSClient

//import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
/**
  * Created by dbhowmick on 8/21/16.
  */
object JobRunner {
  val executorService = Executors.newFixedThreadPool(10)
  implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

  val baseUrl = "https://172.22.64.60:8443/api/v1/views/HIVE/versions/1.5.0/instances/HiveView123"
  val headers = List("X-Requested-By" -> "ambari",
    "Cookie" -> "SUPPORTSESSIONID=7alul7ynu777qsy7j698x37k; AMBARISESSIONID=17xwrkiwxz62v1311lgidfr9fi",
    "Content-Type" -> "application/json")
  val config = new AsyncHttpClientConfigBean()
  config.setAcceptAnyCertificate(true)
  config.setFollowRedirect(true)
  val wsClient = NingWSClient(config)

  def submitJob(jobNumber: Int, statement: String): Future[(String, String)] = {
    println(s"Dipayan >>> Executing statement: $statement")
    val data = Json.obj(
      "job" -> Json.obj(
        "title" -> "Worksheet",
        "dataBase" -> "default",
        "forcedContent" -> statement,
        "referrer" -> "job",
        "globalSettings" -> "set hive.execution.engine=tez;\n"
      )
    )
    wsClient
      .url(s"$baseUrl/jobs")
      .withHeaders("X-Requested-By" -> "ambari",
        "Cookie" -> "SUPPORTSESSIONID=7alul7ynu777qsy7j698x37k; AMBARISESSIONID=17xwrkiwxz62v1311lgidfr9fi",
        "Content-Type" -> "application/json")
      .post(data).map {
      response =>
        ((response.json \ "job" \ "id").as[String], (response.json \ "job" \ "status").as[String])
    }.recoverWith{
      case e => e.printStackTrace(); throw e;
    }
  }

  def getRecurringStatus(jobId: String): Future[(String, String)] = {
    def getStatus(jobId: String): Future[(String, String)] = {
      println(s"Dipayan >>> Calling get status for jobId: $jobId")
      wsClient
        .url(s"$baseUrl/jobs/$jobId")
        .withHeaders("X-Requested-By" -> "ambari",
          "Cookie" -> "SUPPORTSESSIONID=7alul7ynu777qsy7j698x37k; AMBARISESSIONID=17xwrkiwxz62v1311lgidfr9fi",
          "Content-Type" -> "application/json")
        .get()
        .map {
          response =>
            ((response.json \ "job" \ "id").as[String], (response.json \ "job" \ "status").as[String])
        }
    }
    getStatus(jobId).flatMap {
        case (id, "SUCCEEDED") => Future { (id, "SUCCEEDED") }
        case (id, "ERROR") => Future { (id, "ERROR") }
        case (_, st) => Thread.sleep(5000); println(s"Dipayan >>> Retrying to fetch status: jobid: $jobId, status: $st"); getStatus(jobId)
    }
  }

  def getResult(jobId: String, status: String): Future[(String, String)] = {
    def getResult(jobId: String): Future[(String, String)] = {
      println(s"Dipayan >>> Calling response for job id: $jobId")
      wsClient
        .url(s"$baseUrl/jobs/$jobId/results?first=true")
        .withHeaders("X-Requested-By" -> "ambari",
          "Cookie" -> "SUPPORTSESSIONID=7alul7ynu777qsy7j698x37k; AMBARISESSIONID=17xwrkiwxz62v1311lgidfr9fi",
          "Content-Type" -> "application/json")
        .get().map { response =>
          println("Dipayan >>> Response: " + response.json.toString())
          (jobId, "SUCCEEDED")
      }
    }
    status match {
      case "SUCCEEDED" => getResult(jobId)
      case _ => Future { (jobId, status) }
    }
  }

  def runScenario(useless: (String, String), jobNumber: Int, statement: String): Future[(String, String)] = {
    for {
      x <- submitJob(jobNumber, statement)
      submitResult <- getRecurringStatus(x._1)
      result <- getResult(submitResult._1, submitResult._2)
    } yield submitResult
  }

}
