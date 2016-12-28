package main.scala

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Utils {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfFiles(folder: File): List[File] = {
    if (folder.exists && folder.isDirectory) {
      folder.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfFolders(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).toList
    } else {
      List[File]()
    }
  }

  def getListOfFolders(folder: File): List[File] = {
    if (folder.exists && folder.isDirectory) {
      folder.listFiles.filter(_.isDirectory).toList
    } else {
      List[File]()
    }
  }

  def getLines(filePath: String): ArrayBuffer[String] = {
    var arrText = new ArrayBuffer[String]
    Source.fromFile(filePath, "utf-8").getLines().foreach { x => arrText.append(x) }
    arrText
  }
}