import json
import sys
import tempfile

from tornado.testing import *

from delivery.services.metadata_service import MetadataService
from delivery.models.db_models import StagingStatus

from tests.integration_tests.base import BaseIntegration
from tests.test_utils import unorganised_runfolder, assert_eventually_equals


class TestPythonVersion(unittest.TestCase):
    """
    Ensure the python binary is of a compatible version
    """
    REQUIRED_MAJOR_VERSION = 3
    REQUIRED_MINOR_VERSION = 6

    def test_python_binary_version(self):
        self.assertEqual(TestPythonVersion.REQUIRED_MAJOR_VERSION, sys.version_info.major)
        self.assertLessEqual(TestPythonVersion.REQUIRED_MINOR_VERSION, sys.version_info.minor)


class TestIntegration(BaseIntegration):
    def test_can_return_flowcells(self):
        response = self.fetch(self.API_BASE + "/runfolders")

        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        runfolder_names = []
        for runfolder_json in response_json["runfolders"]:
            runfolder_names.append(runfolder_json["name"])

        self.assertIn("160930_ST-E00216_0112_AH37CWALXX", runfolder_names)

        self.assertIn("160930_ST-E00216_0111_BH37CWALXX", runfolder_names)

    def test_can_return_projects(self):
        response = self.fetch(self.API_BASE + "/projects")
        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        first_project = response_json["projects"][0]
        self.assertEqual(first_project["name"], "ABC_123")

    def test_can_organise_project(self):
        runfolder = unorganised_runfolder()
        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix="{}_".format(runfolder.name)) as runfolder_path:
            runfolder = unorganised_runfolder(
                name=os.path.basename(runfolder_path),
                root_path=os.path.dirname(runfolder_path))
            self._create_runfolder_structure_on_disk(runfolder)

            url = "/".join([self.API_BASE, "organise", "runfolder", runfolder.name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 200)

            response_json = json.loads(response.body)
            self.assertEqual(runfolder.path, response_json["runfolder"])
            self.assertListEqual(
                sorted([project.name for project in runfolder.projects]),
                sorted(response_json["projects"]))

            for project in runfolder.projects:
                organised_path = os.path.join(runfolder.path, "Projects", project.name, runfolder.name)
                self.assertTrue(os.path.exists(organised_path))
                checksum_file = os.path.join(organised_path, "checksums.md5")
                samplesheet_file = os.path.join(organised_path, "SampleSheet.csv")
                for f in (checksum_file, samplesheet_file):
                    self.assertTrue(os.path.exists(f))

                checksums = MetadataService.parse_checksum_file(checksum_file)

                def _verify_checksum(file_path, expected_checksum):
                    self.assertIn(file_path, checksums)
                    self.assertEqual(checksums[file_path], expected_checksum)

                _verify_checksum(
                    os.path.join(
                        runfolder.name,
                        os.path.basename(samplesheet_file)),
                    MetadataService.hash_file(samplesheet_file))

                for project_file in project.project_files:
                    project_file_base = os.path.dirname(project.project_files[0].file_path)
                    relative_path = os.path.relpath(project_file.file_path, project_file_base)
                    organised_project_file_path = os.path.join(organised_path, relative_path)
                    self.assertTrue(
                        os.path.samefile(
                            organised_project_file_path,
                            project_file.file_path))
                    _verify_checksum(os.path.join(runfolder.name, relative_path), project_file.checksum)
                for sample in project.samples:
                    sample_path = os.path.join(organised_path, sample.sample_id)
                    self.assertTrue(os.path.exists(sample_path))
                    for sample_file in sample.sample_files:
                        organised_file_path = os.path.join(sample_path, sample_file.file_name)
                        self.assertTrue(os.path.exists(organised_file_path))
                        self.assertTrue(os.path.samefile(sample_file.file_path, organised_file_path))
                        relative_file_path = os.path.join(
                            runfolder.name,
                            os.path.relpath(organised_file_path, organised_path))
                        _verify_checksum(relative_file_path, sample_file.checksum)

    def test_cannot_stage_the_same_runfolder_twice(self):
        # Note that this is a test which skips delivery (since to_outbox is not
        # expected to be installed on the system where this runs)

        with tempfile.TemporaryDirectory(
                dir='./tests/resources/runfolders/',
                prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)
            self._create_checksums_file(tmp_dir)

            url = "/".join([self.API_BASE, "stage", "runfolder", dir_name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 202)

            response = self.fetch(url, method='POST', body='')
            print(response.reason)
            self.assertEqual(response.code, 403)

            # Unless you force the delivery
            response = self.fetch(
                    url,
                    method='POST',
                    body=json.dumps({"force_delivery": True}))
            self.assertEqual(response.code, 202)

    def test_cannot_stage_the_same_project_twice(self):
        # Note that this is a test which skips delivery (since to_outbox is not
        # expected to be installed on the system where this runs)

        with tempfile.TemporaryDirectory(
                dir='./tests/resources/projects') as tmp_dir:

            # Stage once should work
            dir_name = os.path.basename(tmp_dir)
            url = "/".join([self.API_BASE, "stage", "project", dir_name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 202)

            # The second time should not
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 403)

            # Unless you force the delivery
            response = self.fetch(
                    url,
                    method='POST',
                    body=json.dumps({"force_delivery": True}))
            self.assertEqual(response.code, 202)

    def test_can_stage_raw_data(self):
        with tempfile.TemporaryDirectory(
                dir='./tests/resources/runfolders/',
                prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir1, \
            tempfile.TemporaryDirectory(
                dir='./tests/resources/runfolders/',
                prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir2:
            proj_name = "AB-1234"

            for tmp_dir in (tmp_dir1, tmp_dir2):
                self._create_projects_dir_with_random_data(
                        tmp_dir,
                        proj_name=proj_name,
                        )
                self._create_checksums_file(tmp_dir)

            url = "/".join([
                self.API_BASE,
                "stage", "project", "runfolders",
                proj_name])
            payload = {
                    'include_bcl': True,
                    'delivery_mode': 'CLEAN',
                    }

            response = self.fetch(
                    url,
                    method='POST',
                    body=json.dumps(payload))
            self.assertEqual(response.code, 202)

            response_json = json.loads(response.body)
            project, staging_id = next(iter(
                response_json["staging_order_ids"].items()))

            _, link = next(iter(
                response_json["staging_order_links"].items()))

            def get_response():
                status_response = self.fetch(link)
                self.assertEqual(status_response.code, 200)
                return json.loads(status_response.body)["status"]

            assert_eventually_equals(
                    self,
                    1,
                    get_response,
                    StagingStatus.staging_successful.name)

            for tmp_dir in (tmp_dir1, tmp_dir2):
                self.assertTrue(os.path.exists(
                    os.path.join(
                        "/tmp", str(staging_id), project,
                        os.path.basename(tmp_dir),
                        "Raw_Data", "L001", "test_data_L1.bcl")))

    def test_no_raw_data_if_lane_shared(self):
        with tempfile.TemporaryDirectory(
                dir='./tests/resources/runfolders/',
                prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:
            proj_name = "AB-1234"

            self._create_projects_dir_with_random_data(
                    tmp_dir,
                    proj_name=proj_name,
                    share_lane=True,
                    )
            self._create_checksums_file(tmp_dir)

            url = "/".join([
                self.API_BASE,
                "stage", "project", "runfolders",
                proj_name])
            payload = {
                    'include_bcl': True,
                    'delivery_mode': 'CLEAN',
                    }

            response = self.fetch(
                    url,
                    method='POST',
                    body=json.dumps(payload))
            self.assertEqual(response.code, 403)
