package main.scala

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientVertexType
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.orientechnologies.orient.core.metadata.schema.OType

import java.util.HashSet
import java.lang.Iterable
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.Direction

object LDADataFile2OrientDBGraph {

  def main(args: Array[String]): Unit = {
    val rootDir = "C:\\Users\\duytr\\Desktop\\CayChuDe_GiaoDuc\\CayChuDe_GiaoDuc"
    val odbName = "ODBGiaoDuc"
    val topicTopicEdgeLabel = "coChuDe"
    val topicWordEdgeLabel = "coTuKhoa"
    val dataFileName = "DataTrain.txt"

    // opens the DB (if not existing, it will create it)
    val uri: String = "plocal:C:/orientdb/databases/" + odbName
    val factory: OrientGraphFactory = new OrientGraphFactory(uri)
    val graph: OrientGraph = factory.getTx()

    try {

      // if the database does not contain the classes we need (it was just created),
      // then adds them
      if (graph.getVertexType("CHUDE") == null) {

        // we now extend the Vertex class for Person and Company
        val topic: OrientVertexType = graph.createVertexType("CHUDE")
        topic.createProperty("TenChuDe", OType.STRING)

        val word: OrientVertexType = graph.createVertexType("TUKHOA")
        word.createProperty("NoiDung", OType.STRING)

        // we now extend the Edge class for a "CHUDE" relationship
        // between (CHUDE and CHUDE) or (CHUDE and TUKHOA)
        val topictopic: OrientEdgeType = graph.createEdgeType(topicTopicEdgeLabel)
        val topicword: OrientEdgeType = graph.createEdgeType(topicWordEdgeLabel)

        graph.commit()

      } else {
        // cleans up the DB since it was already created in a preceding run
        graph.command(new OCommandSQL("DELETE VERTEX V")).execute()
        graph.command(new OCommandSQL("DELETE EDGE E")).execute()
        graph.commit()
      }

      //create main topic vertex
      val topicGiaoDuc: Vertex = graph.addVertex("class:CHUDE", "TenChuDe", "Giáo dục")

      val listFoldersLevel1 = Utils.getListOfFolders(rootDir)
      listFoldersLevel1.foreach { folder1 =>
        {
          //add vertex of topic and edge between them
          val topicLevel1: Vertex = graph.addVertex("class:CHUDE", "TenChuDe", folder1.getName)
          topicGiaoDuc.addEdge(topicTopicEdgeLabel, topicLevel1)
          graph.commit()
          //add vertex of words and edge between word and topic
          val dataFileDir = folder1.getAbsolutePath + "\\" + dataFileName
          println(dataFileDir)
          val dataLines = Utils.getLines(dataFileDir)
          dataLines.foreach { line =>
            {
              val words = line.split(" ")
              words.foreach { word =>
                {
                  val wordLevel1: Vertex = graph.addVertex("class:TUKHOA", "NoiDung", word)
                  topicLevel1.addEdge(topicWordEdgeLabel, wordLevel1)
                  graph.commit()
                }
              }
            }
          }
          val listFoldersLevel2 = Utils.getListOfFolders(folder1.getAbsoluteFile)
          if (listFoldersLevel2.length > 0)
            listFoldersLevel2.foreach { folder2 =>
              {
                //add vertex of topic and edge between them
                val topicLevel2: Vertex = graph.addVertex("class:CHUDE", "TenChuDe", folder2.getName)
                topicLevel1.addEdge(topicTopicEdgeLabel, topicLevel2)
                graph.commit()
                //add vertex of words and edge between word and topic
                val dataFileDir = folder2.getAbsolutePath + "\\" + dataFileName
                val dataLines = Utils.getLines(dataFileDir)
                dataLines.foreach { line =>
                  {
                    val words = line.split(" ")
                    words.foreach { word =>
                      {
                        val wordLevel2: Vertex = graph.addVertex("class:TUKHOA", "NoiDung", word)
                        topicLevel2.addEdge(topicWordEdgeLabel, wordLevel2)
                        graph.commit()
                      }
                    }
                  }
                }
                val listFoldersLevel3 = Utils.getListOfFolders(folder2.getAbsoluteFile)
                if (listFoldersLevel3.length > 0)
                  listFoldersLevel3.foreach { folder3 =>
                    {
                      //add vertex of topic and edge between them
                      val topicLevel3: Vertex = graph.addVertex("class:CHUDE", "TenChuDe", folder3.getName)
                      topicLevel2.addEdge(topicTopicEdgeLabel, topicLevel3)
                      graph.commit()
                      //add vertex of words and edge between word and topic
                      val dataFileDir = folder3.getAbsolutePath + "\\" + dataFileName
                      val dataLines = Utils.getLines(dataFileDir)
                      dataLines.foreach { line =>
                        {
                          val words = line.split(" ")
                          words.foreach { word =>
                            {
                              val wordLevel3: Vertex = graph.addVertex("class:TUKHOA", "NoiDung", word)
                              topicLevel3.addEdge(topicWordEdgeLabel, wordLevel3)
                              graph.commit()
                            }
                          }
                        }
                      }
                    }
                  }
              }
            }
        }
      }
    } catch {
      case t: Throwable => {
        println("************************ ERROR ************************")
        t.printStackTrace() // TODO: handle error
        println()
        println("************************ ERROR ************************")
      }
    } finally {
      graph.shutdown()
      factory.close()
    }
  }
}