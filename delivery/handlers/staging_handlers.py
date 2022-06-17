
import logging

from tornado.gen import coroutine

from arteria.web.handlers import BaseRestHandler

from delivery.handlers import *
from delivery.exceptions import ProjectNotFoundException, \
        ProjectAlreadyDeliveredException, ForbiddenException

from delivery.models.delivery_modes import DeliveryMode

log = logging.getLogger(__name__)


class BaseStagingHandler(BaseRestHandler):

    def _construct_status_endpoint(self, status_id):
        status_end_point = "{0}://{1}{2}".format(self.request.protocol,
                                                 self.request.host,
                                                 self.reverse_url("stage_status", status_id))
        return status_end_point

    def _construct_response_from_project_and_status(self, staging_order_projects_and_ids):
        link_results = {}
        id_results = {}
        for project, status_id in staging_order_projects_and_ids.items():
            link_results[project] = self._construct_status_endpoint(status_id)
            id_results[project] = status_id

        return link_results, id_results


class StagingProjectRunfoldersHandler(BaseStagingHandler):
    """
    Handler class for handling how to start staging of runfolders belonging to a project. Polling for status,
    canceling, etc can then be handled by the more general `StagingHandler`
    """

    def initialize(self, delivery_service, **kwargs):
        self.delivery_service = delivery_service

    @coroutine
    def post(self, project_id):
        """
        This endpoint allows all runfolders for a specific project to be
        staged. Depending on which `delivery_mode` is specified different
        behaviour will be exhibited. The possible modes are CLEAN, BATCH and
        FORCE. If CLEAN is specified the staging will only be allowed if the
        project has not been delivered before. If BATCH is specified, any
        runfolders which have not previously been staged will be staged. If
        FORCE is specified all runfolders will, regardless of their current
        status, be staged together.

        Here is a python code example of how to call the endpoint.

            import requests
            import json

            url = "http://molmed-43:8080/api/1.0/stage/project/runfolders/ABC_123"

            payload = {'delivery_mode': 'BATCH'}
            headers = {
            'content-type': "application/json",
            }

            response = requests.request("POST", url, data=json.dumps(payload), headers=headers)

            print(response.text)

        """
        log.debug("Trying to stage runfolders for project: {}".format(project_id))

        try:
            request_data = self.body_as_object()
        except ValueError:
            request_data = {}

        if not request_data:
            request_data = {}

        requested_delivery_mode = request_data.get("delivery_mode", None)
        include_bcl = request_data.get("include_bcl", False)

        try:
            delivery_mode = DeliveryMode[requested_delivery_mode]
            log.info(
                "Will attempt to stage runfolders"
                " for project {} with type {}".format(
                    project_id, delivery_mode))

            project_and_stage_id, projects = self.delivery_service \
                .deliver_all_runfolders_for_project(
                        project_id,
                        delivery_mode,
                        include_bcl=include_bcl)

            links, staging_ids_ids = self \
                ._construct_response_from_project_and_status(
                        project_and_stage_id)

            project_and_staged_id_dict = [
                    project.to_dict()
                    for project in projects]

            self.set_status(ACCEPTED)
            self.write_json({
                'staging_order_links': links,
                'staging_order_ids': staging_ids_ids,
                'staged_data': project_and_staged_id_dict})

        except ProjectNotFoundException as e:
            log.warning("Request issued for non-existent project {}".format(
                project_id))
            self.set_status(NOT_FOUND, reason=e.msg)
        except ProjectAlreadyDeliveredException:
            log.warning(
                "Project: {} has already been delivered, and is not compatible"
                " with delivery mode: {}".format(project_id, delivery_mode))
            self.set_status(
                FORBIDDEN,
                reason="This project has already been delivered!"
                " Maybe you want to deliver in BATCH mode instead?"
                " Or if that is not the case you will need to"
                " force the delivery with FORCE")
        except KeyError:
            log.warning("A non-valid delivery mode was requested: {}."
                        " Will deny request.".format(requested_delivery_mode))
            self.set_status(
                    FORBIDDEN,
                    reason="Delivery mode: {} was not permitted."
                    " Only: {} are valid stated".format(
                        requested_delivery_mode,
                        [m.value for m in DeliveryMode]))
        except ForbiddenException as e:
            self.set_status(FORBIDDEN, reason=str(e))


class StagingRunfolderHandler(BaseStagingHandler):
    """
    Handler class for handling how to start staging of a runfolder. Polling for status, canceling, etc can then be
    handled by the more general `StagingHandler`
    """

    def initialize(self, delivery_service, **kwargs):
        self.delivery_service = delivery_service

    @coroutine
    def post(self, runfolder_id):
        """
        Attempt to stage projects from the the specified runfolder, so that they can then be delivered.
        Will return a set of status links, one for each project that can be queried for the status of
        that staging attempt. A list of project names can be specified in the request body to limit which projects
        should be staged. E.g:

            import requests

            url = "http://localhost:8080/api/1.0/stage/runfolder/160930_ST-E00216_0111_BH37CWALXX"

            payload = "{'projects': ['ABC_123']}"
            headers = {
                'content-type': "application/json",
            }

            response = requests.request("POST", url, data=payload, headers=headers)

            print(response.text)

        The return format looks like:
            {"staging_order_links": {"ABC_123": "http://localhost:8080/api/1.0/stage/584"}}

        """

        log.debug("Trying to stage runfolder with id: {}".format(runfolder_id))

        try:
            request_data = self.body_as_object()
        except ValueError:
            request_data = {}

        try:
            projects_to_stage = request_data.get("projects", [])
            force_delivery = request_data.get("force_delivery", False)

            log.debug("Got the following projects to stage: {}".format(projects_to_stage))

            staging_order_projects_and_ids = self.delivery_service.deliver_single_runfolder(runfolder_id,
                                                                                            projects_to_stage,
                                                                                            force_delivery)

            link_results, id_results = self._construct_response_from_project_and_status(staging_order_projects_and_ids)

            self.set_status(ACCEPTED)
            self.write_json({'staging_order_links': link_results,
                             'staging_order_ids': id_results})
        except ProjectNotFoundException as e:
            self.set_status(NOT_FOUND, reason=str(e))
        except ProjectAlreadyDeliveredException as e:
            self.set_status(FORBIDDEN, reason=str(e))


class StageGeneralDirectoryHandler(BaseStagingHandler):
    """
    Handler used to stage projects which are represented as directories in a root directory specified by
    `general_project_directory` in the application config.
    """

    def initialize(self, delivery_service, **kwargs):
        self.delivery_service = delivery_service

    def post(self, directory_name):
        """
        Attempt to stage projects (represented by directories under a configurable root directory),
        so that they can then be delivered.
        Will return a set of status links, one for each project that can be queried for the status of
        that staging attempt. E.g:

            import requests

            url = "http://localhost:8080/api/1.0/stage/project/my_test_project"

            headers = {
                'content-type': "application/json",
            }

            # Optionally send a project alias (when the name of the dir is something else
            than the project name) or force the delivery
            data = {"project_alias": "my_test_project_batch1", "force_delivery": "True"}

            response = requests.request("POST", url, data='', headers=headers)

            print(response.text)

        The return format looks like:
            {"staging_order_links": {"my_test_project": "http://localhost:8080/api/1.0/stage/591"}}

        """
        try:
            request_data = self.body_as_object()
        except ValueError:
            request_data = {}

        # body as object will return None if no data is given
        if not request_data:
            request_data = {}

        project_alias = request_data.get("project_alias", None)
        force_delivery = request_data.get("force_delivery", False)

        try:
            stage_order_and_id = self.delivery_service.\
                deliver_arbitrary_directory_project(project_name=directory_name,
                                                    dir_name=project_alias,
                                                    force_delivery=force_delivery)

            link_results, id_results = self._construct_response_from_project_and_status(stage_order_and_id)

            self.set_status(ACCEPTED)
            self.write_json({'staging_order_links': link_results,
                             'staging_order_ids': id_results})
        except ProjectAlreadyDeliveredException as e:
            self.set_status(FORBIDDEN, reason=str(e))

class StagingHandler(BaseRestHandler):

    def initialize(self, delivery_service, **kwargs):
        self.delivery_service = delivery_service

    def get(self, stage_id):
        """
        Returns the current status as json of the of the staging order, or 404 if the order is unknown.
        Possible values for status are: pending, staging_in_progress, staging_successful, staging_failed
        Return format looks like:
        {
           "status": "staging_successful"
        }
        """
        stage_order = self.delivery_service.check_staging_status(stage_id)
        if stage_order:
            self.write_json({'status': stage_order.status.name, 'size': stage_order.size})
        else:
            self.set_status(NOT_FOUND, reason='No stage order with id: {} found.'.format(stage_id))

    def delete(self, stage_id):
        """
        Kill a stage order with the give id. Will return status 204 if the staging process was successfully cancelled,
        otherwise it will return status 500.
        """
        was_killed = self.delivery_service.kill_process_of_stage_order(stage_id)
        if was_killed:
            self.set_status(NO_CONTENT)
        else:
            self.set_status(INTERNAL_SERVER_ERROR,
                            reason="Could not kill stage order with id: {}, either it wasn't in a state "
                                   "which allows it to be killed, or the pid associated with the stage order "
                                   "did not allow itself to be killed. Consult the server logs for an exact "
                                   "reason.")
