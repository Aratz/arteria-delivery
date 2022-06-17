

class RunfolderNotFoundException(Exception):
    """
    Should be raised when a runfolder is not found.
    """
    pass


class ChecksumNotFoundException(Exception):
    """
    Should be raised when a file checksum could not be found in the list of
    checksums.
    """
    pass


class ChecksumFileNotFoundException(Exception):
    """
    Should be raised when an expected checksum file could not be found.
    """
    pass


class ProjectNotFoundException(Exception):
    """
    Should be raised when and invalid or non-existent project is searched for.
    """
    pass


class ProjectReportNotFoundException(Exception):
    """
    Should be raised when and invalid or non-existent project is searched for.
    """
    pass


class TooManyProjectsFound(Exception):
    """
    Should be raise when to many projects match some specific criteria.
    """
    pass


class InvalidStatusException(Exception):
    """
    Should be raised when an object is found to be in a invalid state, e.g. if
    the program tries to start staging on a StagingOrder which is already
    `in_progress`.
    """
    pass


class ProjectAlreadyDeliveredException(Exception):
    """
    Should be raised when a project has already been delivered.
    """
    pass


class ProjectAlreadyOrganisedException(Exception):
    """
    Should be raised when a project has already been organised.
    """
    pass


class FileNameParsingException(Exception):
    pass


class SamplesheetNotFoundException(Exception):
    pass


class ProjectsDirNotfoundException(Exception):
    """
    Should be raised when a directory containing projects could not be found.
    """
    pass


class CannotParseDDSOutputException(Exception):
    """
    Should be raised when DDS's output cannot be parsed for e.g. creating a
    project.
    """
    pass


class ForbiddenException(Exception):
    """
    Should be raised when something forbidden is attempted and should result in
    the 403 response code.
    """
