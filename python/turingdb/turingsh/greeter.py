import re

from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.shortcuts import print_formatted_text


def get_visible_width(text):
    """Remove HTML-like tags and get actual visible character count"""
    return len(re.sub(r"<[^>]*>", "", text)) - 2


def center_with_html(text, total_width):
    """Center text accounting for HTML markup"""
    visible_width = get_visible_width(text)
    padding = (total_width - visible_width) // 2
    return " " * padding + text + " " * (total_width - visible_width - padding)


def greet():

    topbar = (
        "<skyblue>+=====================================================+</skyblue>\n"
    )
    botbar = (
        "<skyblue>+=====================================================+</skyblue>"
    )
    delimiter = "<skyblue>|</skyblue>"
    title = "<b>TuringDB Interactive Shell</b>"
    help_msg = "Type <i>help</i> for help"
    quit_msg = "Type <i>quit</i> or press <i>Ctrl+D</i> to quit"

    box_content_width = 51

    print_formatted_text(
        HTML(
            f"{topbar}"
            f"{delimiter}{center_with_html(title, box_content_width)}{delimiter}\n"
            f"{delimiter}{center_with_html(help_msg, box_content_width)}{delimiter}\n"
            f"{delimiter}{center_with_html(quit_msg, box_content_width)}{delimiter}\n"
            f"{botbar}"
        )
    )
