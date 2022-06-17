
import os
import csv
import logging

from delivery.services.file_system_service import FileSystemService
from delivery.exceptions import ProjectAlreadyDeliveredException, \
        ProjectNotFoundException, ForbiddenException
from delivery.models.delivery_modes import DeliveryMode


log = logging.getLogger(__name__)


class DeliveryService(object):

    def __init__(self,
                 delivery_sources_repo,
                 general_project_repo,
                 runfolder_service,
                 staging_service,
                 dds_service,
                 project_links_directory,
                 file_system_service=FileSystemService()):
        self.delivery_sources_repo = delivery_sources_repo
        self.staging_service = staging_service
        self.dds_service = dds_service
        self.general_project_repo = general_project_repo
        self.runfolder_service = runfolder_service
        self.project_links_directory = project_links_directory
        self.file_system_service = file_system_service

    def _validate_source_and_add_to_repo(self, source, force_delivery, path):
        source_exists = self.delivery_sources_repo.source_exists(source)

        # If such a Delivery source exists, only proceed if
        # override is activated
        if source_exists and not force_delivery:
            raise ProjectAlreadyDeliveredException(
                "Project source {} has already been delivered.".format(source))
        elif source_exists and force_delivery:
            self.delivery_sources_repo.update_path_of_source(source, new_path=path)
        else:
            self.delivery_sources_repo.add_source(source)

    def _validate_and_stage_source(self, source, force_delivery, path, project_name):
        self._validate_source_and_add_to_repo(source, force_delivery, path)
        # Start staging
        stage_order = self.staging_service.create_new_stage_order(path=source.path, project_name=project_name)
        self.staging_service.stage_order(stage_order)
        return stage_order

    def _start_staging_projects(self, projects, force_delivery):
        projects_and_stage_order_ids = {}
        for project in projects:
            source = self.delivery_sources_repo.create_source(project_name=project.name,
                                                              source_name="{}/{}".format(project.runfolder_name,
                                                                                         project.name),
                                                              path=project.path)
            stage_order = self._validate_and_stage_source(source, force_delivery, project.path, project.name)
            projects_and_stage_order_ids[project.name] = stage_order.id

        return projects_and_stage_order_ids

    def _create_links_area_for_project_runfolders(self, project_name, projects, batch_nbr):
        """
        Creates a directory in which it creates links to all runfolders for the
        projects given. This is useful so that we can then rsync that directory
        to the staging area.
        :param project_name: name of the project
        :param projects: runfolders with the specified project on them
        :param batch_nbr: which batch of deliveries of this project this
        corresponds to
        :return: the path to the dir created
        """

        project_dir = os.path.join(
                self.project_links_directory,
                project_name,
                str(batch_nbr))
        try:
            self.file_system_service.makedirs(project_dir)
        except FileExistsError as e:
            log.error("Project dir: {} already exists".format(project_dir))
            raise e

        for project in projects:
            try:
                link_name = os.path.join(project_dir, project.runfolder_name)
                self.file_system_service.symlink(project.path, link_name)
            except FileExistsError as e:
                log.error(
                        "Project link: {} already exists".format(project_dir))
                raise e

        return self.file_system_service.abspath(project_dir)

    def _get_project_lanes(self, project):
        with open(os.path.join(
                project.runfolder_path,
                'SampleSheet.csv')) as sample_sheet:
            # Discard Header & Settings information
            for line in sample_sheet:
                if '[Data]' in line:
                    break

            csv_reader = csv.DictReader(sample_sheet, delimiter=',')
            project_lanes, foreign_lanes = set(), set()
            for row in csv_reader:
                if row['Sample_Project'] == project.name:
                    project_lanes.add(row['Lane'])
                else:
                    foreign_lanes.add(row['Lane'])

        # Check the lanes used by our project are not shared with another
        # project
        if project_lanes.intersection(foreign_lanes):
            error_msg = (
                    "Cannot deliver raw data if the project"
                    " does not have its own lanes."
                    )
            log.error(error_msg)
            raise ForbiddenException(error_msg)

        return [int(lane) for lane in project_lanes]

    def _link_raw_files(
            self,
            links_directory,
            projects,
            ):

        for project in projects:
            lane_folders = [
                    f"Data/Intensities/BaseCalls/L{lane:03d}"
                    for lane in self._get_project_lanes(project)]

            for lane_folder in lane_folders:
                raw_datafiles = os.path.join(
                        project.runfolder_path,
                        lane_folder
                        )
                link_name = os.path.join(
                        links_directory,
                        project.runfolder_name,
                        "Raw_Data",
                        os.path.basename(lane_folder),
                        )

                self.file_system_service.symlink(raw_datafiles, link_name)

    def deliver_single_runfolder(
            self,
            runfolder_name,
            only_these_projects,
            force_delivery):
        runfolder = self.runfolder_service.find_runfolder(runfolder_name)
        projects = list(self.runfolder_service.find_projects_on_runfolder(
            runfolder,
            only_these_projects))
        return self._start_staging_projects(projects, force_delivery)

    def _get_projects_to_deliver(self, projects, mode, batch_nbr):
        # First create sources for all the projects, depending on mode
        # The make a separate delivery source for the actually stages folder...
        for project in projects:
            source = self.delivery_sources_repo.create_source(
                project_name=project.name,
                source_name="{}/{}".format(
                    project.runfolder_name,
                    project.name),
                path=project.path,
                batch_nbr=batch_nbr)
            try:
                if mode is DeliveryMode.FORCE:
                    force_delivery = True
                else:
                    force_delivery = False

                self._validate_source_and_add_to_repo(
                    source,
                    force_delivery=force_delivery,
                    path=project.path)
                yield project
            except ProjectAlreadyDeliveredException as e:
                log.debug(
                    "Source {} has already been delivered".format(source))
                if mode == DeliveryMode.CLEAN:
                    log.debug(
                        "Clean mode running, don't proceed as"
                        " this source has already been delivered.")
                    raise e
                elif mode == DeliveryMode.BATCH:
                    log.debug(
                        "Batch mode running, do not include"
                        " this source in the delivery")
                    continue
                elif mode == DeliveryMode.FORCE:
                    log.debug(
                        "Force mode running, include this source"
                        " even if it has been delivered before.")
                    self.delivery_sources_repo.update_path_of_source(
                        source,
                        project.path)
                    yield project
                else:
                    raise NotImplementedError(
                            "This is not a valid state,"
                            " delivery mode needs to be CLEAN/BATCH/FORCE.")

    def deliver_all_runfolders_for_project(
            self,
            project_name,
            mode,
            include_bcl=False):
        """
        This method will attempt to deliver all runfolders for the specified
        project.

        Since the process is somewhat involved, here's a explanation of what's
        going on and why.

        First, there are three modes of delivery which needs to be handled.
        CLEAN, which denotes that this project is not allowed to be delivered
        previously. BATCH, which will deliver any runfolders which have not
        previously been delivered. And finally, FORCE, which will deliver all
        the runfolders regardless of their previous status.

        Two steps are then required to enable the staging, that require some
        explanation.  Reading the code you will note that the
        _get_projects_to_deliver will create a DeliverySource and then a new
        DeliverySource will be created by this method. The reason for this is
        that since we create a intermediate directory in which links to all the
        runfolders which are to be delivered together are created. This
        directory is then passed as a DeliverySource when creating a new
        StagingOrder (which is goes on to be staged).

        :param project_name: of project to deliver
        :param mode: A DeliveryMode
        :return: a tupple with a dict with {<project name>: <staging order id>}, and the projects
        """
        projects = list(self.runfolder_service.find_runfolders_for_project(
            project_name))

        if len(projects) < 1:
            raise ProjectNotFoundException(
                    "Could not find any Project "
                    "folders for project name: {}".format(project_name))

        max_batch_nbr = self.delivery_sources_repo.find_highest_batch_nbr(
                project_name)

        if not max_batch_nbr:
            batch_nbr = 1
        else:
            batch_nbr = max_batch_nbr + 1

        projects_to_deliver = list(self._get_projects_to_deliver(
            projects,
            mode,
            batch_nbr))

        if not projects_to_deliver:
            raise ProjectAlreadyDeliveredException(
                    "All runfolders for this project has already "
                    "been delivered.")

        log.debug("The following projects were to be delivered: {}".format(
            projects_to_deliver))
        log.debug("This will be batch nbr: {}".format(batch_nbr))

        links_directory = self._create_links_area_for_project_runfolders(
                project_name,
                projects_to_deliver,
                batch_nbr)

        if include_bcl:
            self._link_raw_files(
                    links_directory,
                    projects_to_deliver,
                )

        source = self.delivery_sources_repo.create_source(
                project_name=project_name,
                source_name="{}/batch{}".format(project_name, batch_nbr),
                path=links_directory,
                batch_nbr=batch_nbr)

        self.delivery_sources_repo.add_source(source)

        stage_order = self.staging_service.create_new_stage_order(
                path=source.path,
                project_name=project_name)
        self.staging_service.stage_order(stage_order)
        return {source.project_name: stage_order.id}, projects_to_deliver

    def deliver_arbitrary_directory_project(self, project_name, dir_name=None, force_delivery=False):

        if not dir_name:
            dir_name = project_name

        # Construct DeliverySource for the project
        project = self.general_project_repo.get_project(dir_name)
        # Check if such a DeliverySource already exists.

        source = self.delivery_sources_repo.create_source(project_name=project_name,
                                                          source_name=os.path.basename(project.path),
                                                          path=project.path)

        stage_order = self._validate_and_stage_source(source, force_delivery, project.path, project_name)
        return {source.project_name: stage_order.id}

    def check_staging_status(self, staging_id):
        stage_order = self.staging_service.get_stage_order_by_id(staging_id)
        return stage_order

    def kill_process_of_stage_order(self, staging_id):
        was_killed = self.staging_service.kill_process_of_stage_order(staging_id)
        return was_killed
