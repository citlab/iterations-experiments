package de.tuberlin.cit.experiments.iterations.cli.command

import java.lang.{System => Sys}

import anorm.SqlParser._
import anorm._
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.loadConfig
import org.peelframework.core.results.DB
import org.peelframework.core.util.console._
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Query the database for the runtimes of a particular experiment. */
@Service("wc:query-runtimes")
class QueryRuntimes extends Command {

  import scala.language.postfixOps

  override val name = "wc:query-runtimes"

  override val help = "query the database for the runtimes of a particular experiment."

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--connection")
      .`type`(classOf[String])
      .dest("app.db.connection")
      .metavar("ID")
      .help("database config name (default: h2)")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("experiments suite to run")

    // option defaults
    parser.setDefault("app.db.connection", "h2")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.db.connection", ns.getString("app.db.connection"))
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
  }

  override def run(context: ApplicationContext) = {
    logger.info(s"Querying runtime results for suite '${Sys.getProperty("app.suite.name")}' from '${Sys.getProperty("app.db.connection")}'")

    // load application configuration
    implicit val config = loadConfig()

    // create database connection
    val connName = Sys.getProperty("app.db.connection")
    implicit val conn = DB.getConnection(connName)

    val suite = config.getString("app.suite.name")

    try {
      val rowParser = {
        get[String]          ("suite")              ~
        get[String]          ("name")               ~
        get[Int]             ("median_time")        map {
          case s ~ n ~ t => (s, n, t)
        }
      }

      // Create an SQL query
      val runtimes = SQL(
          """
          |SELECT   e.suite         as suite,
          |         e.name          as name,
          |         MEDIAN(er.time) as median_time,
          |         MIN(er.time) as min_time,
          |         MAX(er.time) as max_time
          |FROM     experiment      e,
          |         experiment_run  er
          |WHERE    e.id = er.experiment_id
          |AND      e.suite = {suite}
          |GROUP BY e.suite, e.name
          |ORDER BY e.suite, e.name
          """.stripMargin.trim
        )
        .on("suite" -> suite)
        .as({
          get[String] ("suite")       ~
          get[String] ("name")        ~
          get[Int]    ("median_time") ~
          get[Int]    ("min_time") ~
          get[Int]    ("max_time") map {
            case s ~ n ~ median ~ min ~ max => (s, n, median, min, max)
          }
        } * )

      logger.info(s"------------------------------------------------------------------------------------------------")
      logger.info(s"| RUNTIME RESULTS FOR '$suite' ${" " * (69 - suite.length)} |")
      logger.info(s"------------------------------------------------------------------------------------------------")
      logger.info(s"| name                      | name                      |     median |        min |        max |")
      logger.info(s"------------------------------------------------------------------------------------------------")
      for ((suite, name, median, min, max) <- runtimes) {
        logger.info(f"| $suite%-25s | $name%-25s | $median%10d | $min%10d | $max%10d | ")
      }
      logger.info(s"------------------------------------------------------------------------------------------------")
    }
    catch {
      case e: Throwable =>
        logger.error(s"Error while querying runtime results for suite '${Sys.getProperty("app.suite.name")}'".red)
        throw e
    } finally {
      logger.info(s"Closing connection to database '$connName'")
      conn.close()
      logger.info("#" * 60)
    }
  }
}
