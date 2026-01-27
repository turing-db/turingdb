from functools import wraps

from click import ClickException


def shell_command(click_cmd):
    """
    Decorator that wraps a Click command to be callable from a shell
    with exception handling instead of sys.exit().
    """

    @wraps(click_cmd)
    def wrapper(client, *args, **kwargs):
        try:
            return click_cmd.main(
                args=list(args),
                prog_name=click_cmd.name,
                obj=client,
                standalone_mode=False,
            )
        except ClickException as e:
            raise RuntimeError(e.format_message()) from e

    return wrapper
