
from tornado.web import URLSpec as url

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from alembic.config import Config as AlembicConfig
from alembic.command import upgrade as upgrade_db

from arteria.web.app import AppService

from delivery.handlers.utility_handlers import VersionHandler
from delivery.handlers.runfolder_handlers import RunfolderHandler
from delivery.handlers.project_handlers import ProjectHandler, ProjectsForRunfolderHandler
from delivery.handlers.delivery_handlers import DeliverRunfolderHandler
from delivery.handlers.staging_handlers import StagingRunfolderHandler, StagingHandler

from delivery.repositories.runfolder_repository import FileSystemBasedRunfolderRepository
from delivery.repositories.staging_repository import DatabaseBasedStagingRepository

from delivery.services.delivery_service import MoverDeliveryService
from delivery.services.external_program_service import ExternalProgramService
from delivery.services.staging_service import StagingService


def routes(**kwargs):
    """
    Setup routes and feed them any kwargs passed, e.g.`routes(config=app_svc.config_svc)`
    Help will be automatically available at /api, and will be based on the
    doc strings of the get/post/put/delete methods
    :param: **kwargs will be passed when initializing the routes.
    """
    return [
        url(r"/api/1.0/version", VersionHandler, name="version", kwargs=kwargs),

        url(r"/api/1.0/runfolders", RunfolderHandler, name="runfolder", kwargs=kwargs),
        url(r"/api/1.0/projects", ProjectHandler, name="projects", kwargs=kwargs),
        url(r"/api/1.0/runfolder/(.+)/projects", ProjectsForRunfolderHandler,
            name="projects_for_runfolder", kwargs=kwargs),


        url(r"/api/1.0/stage/runfolder/(.+)", StagingRunfolderHandler,
            name="stage_runfolder", kwargs=kwargs),

        url(r"/api/1.0/stage/(\d+)", StagingHandler, name="stage_status", kwargs=kwargs),

        # TODO Should deliver by stage id!
        url(r"/api/1.0/deliver/runfolder/(.+)", DeliverRunfolderHandler,
            name="delivery_runfolder", kwargs=kwargs)
    ]


def create_and_migrate_db(db_engine, db_connection_string):
    # TODO Do not hard code
    alembic_cfg = AlembicConfig('config/alembic.ini')
    alembic_cfg.set_main_option("url", db_connection_string)
    alembic_cfg.set_main_option("script_location", "alembic/")

    with db_engine.begin() as connection:
        alembic_cfg.attributes["connection"] = connection
        upgrade_db(alembic_cfg, "head")

def compose_application(config):
    runfolder_repo = FileSystemBasedRunfolderRepository(
        config["monitored_directory"])
    external_program_service = ExternalProgramService()

    db_connection_string = config["db_connection_string"]
    engine = create_engine(db_connection_string, echo=False)

    create_and_migrate_db(engine, db_connection_string)

    session_factory = scoped_session(sessionmaker())
    session_factory.configure(bind=engine)

    staging_repo = DatabaseBasedStagingRepository(session_factory=session_factory)

    staging_service = StagingService(external_program_service=external_program_service,
                                     runfolder_repo=runfolder_repo,
                                     staging_repo=staging_repo,
                                     staging_dir=config["staging_directory"],
                                     session_factory=session_factory)

    delivery_service = MoverDeliveryService(external_program_service)

    return dict(config=config,
                runfolder_repo=runfolder_repo,
                external_program_service=external_program_service,
                staging_service=staging_service,
                delivery_service=delivery_service)


def start():
    """
    Start the delivery-ws app
    """
    app_svc = AppService.create(__package__)
    config = app_svc.config_svc

    composed_service = compose_application(config)

    app_svc.start(routes(**composed_service))
