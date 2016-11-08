

class RunfolderNotFoundException(Exception):
    """
    Should be raised when a runfolder is not found
    """
    pass


class InvalidStatusException(Exception):
    """
    Should be raised when an object is found to be in a invalid state, e.g. if the program tries to start staging
    on a StagingOrder which is already `in_progress`
    """
    pass
