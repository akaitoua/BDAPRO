import com.google.inject.AbstractModule
import java.time.Clock

import services.{InitDatabase}
import controllers.{DatasetDBController, FilesController, IntegrationDBController}
import javax.inject.Inject
import org.h2.jdbc.JdbcSQLException
import play.api.Logger
import play.api.db.Databases
import play.api.db._
import play.api.db.evolutions.Evolutions

/**
 * This class is a Guice module that tells Guice how to bind several
 * different types. This Guice module is created when the Play
 * application starts.

 * Play will automatically use any class called `Module` that is in
 * the root package. You can create modules in other locations by
 * adding `play.modules.enabled` settings to the `application.conf`
 * configuration file.
 */
class Module extends AbstractModule {

  override def configure() = {
    // Initialize the DB
    bind(classOf[InitDatabase]).asEagerSingleton()
  }

}
