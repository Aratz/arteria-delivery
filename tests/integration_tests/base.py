import os
import csv
import json
import mock
import random
import logging

from subprocess import PIPE

from tornado.testing import *
from tornado.web import Application

from arteria.web.app import AppService

from delivery.app import routes as app_routes, compose_application
from delivery.services.metadata_service import MetadataService
from delivery.models.execution import Execution

from tests.test_utils import samplesheet_file_from_runfolder

log = logging.getLogger(__name__)


class BaseIntegration(AsyncHTTPTestCase):
    def __init__(self, *args):
        super().__init__(*args)

        self.mock_delivery = True

        # Default duration of mock delivery
        self.mock_duration = 0.1

    def _create_projects_dir_with_random_data(
            self,
            base_dir,
            proj_name='ABC_123',
            share_lane=False,
            ):
        tmp_proj_dir = os.path.join(base_dir, 'Projects', proj_name)
        os.makedirs(tmp_proj_dir)
        with open(os.path.join(tmp_proj_dir, 'test_file'), 'wb') as f:
            f.write(os.urandom(1024))

        N_LANES = 2
        with open(os.path.join(base_dir, "SampleSheet.csv"), 'w') as f:
            f.write("[Data],,,,,\n")
            fieldnames = [
                    "Lane",
                    "Sample_ID", "Sample_Name", "Sample_Project",
                    "Index_ID", "index", "index2"]
            csvwriter = csv.DictWriter(f, fieldnames=fieldnames)
            csvwriter.writeheader()
            csvwriter.writerow({"Lane": 1, "Sample_Project": proj_name})
            csvwriter.writerow({"Lane": 2, "Sample_Project": "XYZ_456"})

            if share_lane:
                csvwriter.writerow({
                    "Lane": 2, "Sample_Project": proj_name})

        for lane in range(1, N_LANES + 1):
            tmp_data_dir = os.path.join(
                    base_dir,
                    "Data", "Intensities", "BaseCalls",
                    f"L{lane:03d}")
            os.makedirs(tmp_data_dir)
            with open(
                    os.path.join(tmp_data_dir, f'test_data_L{lane}.bcl'),
                    'wb') as f:
                f.write(os.urandom(16))

    @staticmethod
    def _create_checksums_file(base_dir, checksums=None):
        checksum_file = os.path.join(base_dir, "MD5", "checksums.md5")
        os.mkdir(os.path.dirname(checksum_file))
        MetadataService.write_checksum_file(checksum_file, checksums or {})
        return checksum_file

    @staticmethod
    def _create_runfolder_structure_on_disk(runfolder):

        def _toggle():
            state = True
            while True:
                yield state
                state = not state

        def _touch_file(file_path):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(os.urandom(1024))

        os.makedirs(runfolder.path, exist_ok=True)
        for project in runfolder.projects:
            os.makedirs(project.path)
            for sample in project.samples:
                for sample_file in sample.sample_files:
                    _touch_file(sample_file.file_path)
            for report_file in project.project_files:
                _touch_file(report_file.file_path)

        checksum_file = os.path.join(runfolder.path, "MD5", "checksums.md5")
        os.mkdir(os.path.dirname(checksum_file))
        MetadataService.write_checksum_file(checksum_file, runfolder.checksums)
        samplesheet_file_from_runfolder(runfolder)

    API_BASE = "/api/1.0"

    def get_app(self):

        # Get an as similar app as possible, tough note that we don't use the
        #  app service start method to start up the the application
        path_to_this_file = os.path.abspath(
            os.path.dirname(os.path.realpath(__file__)))
        app_svc = AppService.create(
                product_name="test_delivery_service",
                config_root="{}/../../config/".format(path_to_this_file),
                args=[])

        config = app_svc.config_svc
        composed_application = compose_application(config)
        routes = app_routes(**composed_application)

        if self.mock_delivery:
            def mock_delivery(cmd):
                project_id = f"snpseq{random.randint(0, 10**10):010d}"
                log.debug(f"Mock is called with {cmd}")
                shell = False
                if cmd[0].endswith('dds'):
                    new_cmd = ['sleep', str(self.mock_duration)]

                    if 'project' in cmd:
                        dds_output = f"""Current user: bio
        Project created with id: {project_id}
        User forskare was associated with Project {project_id} as Owner=True. An e-mail notification has not been sent.
        Invitation sent to email@adress.com. The user should have a valid account to be added to a
        project"""
                        new_cmd += ['&&', 'echo', f'"{dds_output}"']
                        new_cmd = " ".join(new_cmd)
                        shell = True
                    elif cmd[-2:] == ['ls', '--json']:
                        new_cmd = ['sleep', str(0.01)]
                        dds_output = json.dumps([{
                                    "Access": True,
                                    "Last updated": "Fri, 01 Jul 2022 14:31:13 CEST",
                                    "PI": "pi@email.com",
                                    "Project ID": "snpseq00025",
                                    "Size": 25856185058,
                                    "Status": "In Progress",
                                    "Title": "AB1234"
                                    }])
                        new_cmd += ['&&', 'echo', f"'{dds_output}'"]
                        new_cmd = " ".join(new_cmd)
                        shell = True
                    elif 'put' in cmd:
                        source_file = cmd[cmd.index("--source") + 1]
                        auth_token = cmd[cmd.index("--token-path") + 1]
                        new_cmd += ['&&', 'test', '-e', source_file]
                        new_cmd += ['&&', 'test', '-e', auth_token]
                        new_cmd = " ".join(new_cmd)
                        shell = True
                else:
                    new_cmd = cmd

                log.debug(f"Running mocked {new_cmd}")
                p = Subprocess(new_cmd,
                               stdout=PIPE,
                               stderr=PIPE,
                               stdin=PIPE,
                               shell=shell)
                return Execution(pid=p.pid, process_obj=p)

            self.patcher = mock.patch(
                    'delivery.services.external_program_service'
                    '.ExternalProgramService.run',
                    wraps=mock_delivery)

        return Application(routes)

    def setUp(self):
        super().setUp()
        try:
            self.patcher.start()
        except AttributeError:
            pass

    def tearDown(self):
        try:
            self.patcher.stop()
        except AttributeError:
            pass
        super().tearDown()
