import traceback


def backoff_hdlr(details):
    # traceback.format_exc(details["exception"])

    if "list index" in str(details["exception"]):
        traceback.print_exc()
        return

    print(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}. Exception: {exception}".format(**details)
    )
