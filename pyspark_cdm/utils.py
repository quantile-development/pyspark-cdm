from cdm.enums import CdmStatusLevel


def event_callback(status_level: CdmStatusLevel, message: str) -> None:
    """
    Event callback function for CDM.

    Args:
        status_level (CdmStatusLevel): Status level of the message.
        message (str): Message to be printed.
    """
    print(message)
