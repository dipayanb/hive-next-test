package com.dipayan.hive

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
/**
  * Created by dbhowmick on 8/21/16.
  */
object TestHive extends App {
  scenario1CreateDropDb
  // scenario2select
  // scenario3select
  // scenario4select

  def scenario2select: Unit = {
    for (j <- 1 to 5) {
      val all = (1 to 3).map { i =>
        for {
          a <- JobRunner.runScenario(("x", "x"), i, s"select * from geolocation order by event limit 5")
        //b <- JobRunner.runScenario(a, i, s"drop database dipayandb$i")
        } yield a
      }

      Await.result(Future.sequence(all), Duration.Inf)
    }
  }

  def scenario3select: Unit = {
    for (j <- 1 to 10) {
      val all = (1 to 5).map { i =>
        for {
          a <- JobRunner.runScenario(("x", "x"), i, s"select * from geolocation  limit 5")
        //b <- JobRunner.runScenario(a, i, s"drop database dipayandb$i")
        } yield a
      }

      Await.result(Future.sequence(all), Duration.Inf)
    }
  }

  def scenario4select: Unit = {
    for (j <- 1 to 10) {
      val all = (1 to 5).map { i =>
        for {
          a <- JobRunner.runScenario(("x", "x"), i, s"select count(*) from geolocation")
        //b <- JobRunner.runScenario(a, i, s"drop database dipayandb$i")
        } yield a
      }

      Await.result(Future.sequence(all), Duration.Inf)
    }
  }

  def scenario1CreateDropDb: Unit = {
    for (j <- 1 to 10) {
      val all = (1 to 10).map { i =>
        for {
          a <- JobRunner.runScenario(("x", "x"), i, s"create database dipayandb$i")
          b <- JobRunner.runScenario(a, i, s"drop database dipayandb$i")
        } yield b
      }

      Await.result(Future.sequence(all), Duration.Inf)
    }
  }



}
