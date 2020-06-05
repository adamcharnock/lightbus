# pylint: skip-file
"""Lightbus logging setup

Credit to: https://github.com/borntyping/python-colorlog

This has been vendored to minimise dependencies and allow for
customisation in the long term.

This needs some serious cleanup: https://github.com/adamcharnock/lightbus/issues/10
"""

import logging
import sys

__all__ = ("escape_codes", "default_log_colors", "LightbusFormatter")

# The default colors to use for the debug levels
default_log_colors = {
    "DEBUG": "white",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}

# The default format to use for each style
default_formats = {
    "%": {
        "DEBUG": "%(log_color)s%(threadName)s | %(name)s | %(msg)s",
        "INFO": "%(log_color)s%(threadName)s | %(name)s | %(msg)s",
        "WARNING": "%(log_color)s%(threadName)s | %(name)s | %(msg)s",
        "ERROR": "%(log_color)s%(threadName)s | %(name)s | ERROR: %(msg)s (%(module)s:%(lineno)d)",
        "CRITICAL": "%(log_color)s%(threadName)s | %(name)s | CRITICAL: %(msg)s",
    },
    "{": "{log_color} {levelname}:{name}:{message}",
    "$": "${log_color}$ ${levelname}:${name}:${message}",
}


def esc(*x):
    return "\033[" + ";".join(x) + "m"


escape_codes = {"reset": esc("0"), "bold": esc("01"), "thin": esc("02")}

# The color names
COLORS = ["black", "red", "green", "yellow", "blue", "purple", "cyan", "white"]

PREFIXES = [
    # Foreground without prefix
    ("3", ""),
    ("01;3", "bold_"),
    ("02;3", "thin_"),
    # Foreground with fg_ prefix
    ("3", "fg_"),
    ("01;3", "fg_bold_"),
    ("02;3", "fg_thin_"),
    # Background with bg_ prefix - bold/light works differently
    ("4", "bg_"),
    ("10", "bg_bold_"),
]

for prefix, prefix_name in PREFIXES:
    for code, name in enumerate(COLORS):
        escape_codes[prefix_name + name] = esc(prefix + str(code))


def parse_colors(sequence):
    """Return escape codes from a color sequence."""
    return "".join(escape_codes[n] for n in sequence.split(",") if n)


class LightbusLogRecord:  # pragma: no cover
    """
    Wraps a LogRecord, adding named escape codes to the internal dict.

    The internal dict is used when formatting the message (by the PercentStyle,
    StrFormatStyle, and StringTemplateStyle classes).
    """

    def __init__(self, record):
        """Add attributes from the escape_codes dict and the record."""
        self.is_tty = True
        self.additional_line_prefix = ""
        self.msg_cb = None
        record.name = record.name.ljust(30)

        self.__dict__.update(escape_codes)
        self.__dict__.update(record.__dict__)

        # Keep a reference to the original record so ``__getattr__`` can
        # access functions that are not in ``__dict__``
        self._record = record

    def getMessage(self):
        # Render the message if necessary (i.e. because it is an instance of the L class)
        if hasattr(self.msg, "render"):
            self.msg = self.msg.render(
                tty=self.is_tty,
                additional_line_prefix=self.additional_line_prefix,
                style=self.log_color,
                msg_cb=self.msg_cb,
            )
        else:
            if self.msg_cb:
                self.msg = self.msg_cb(self.msg, is_tty=self.is_tty)

        return self._record.getMessage()

    def __getattr__(self, name):
        return getattr(self._record, name)


class LightbusFormatter(logging.Formatter):  # pragma: no cover
    """
    A formatter that allows colors to be placed in the format string.

    Intended to help in creating more readable logging output.
    """

    def __init__(
        self,
        fmt=None,
        datefmt=None,
        style="%",
        log_colors=None,
        reset=True,
        secondary_log_colors=None,
        stream=None,
    ):
        """
        Set the format and colors the LightbusFormatter will use.

        The ``fmt``, ``datefmt`` and ``style`` args are passed on to the
        ``logging.Formatter`` constructor.

        The ``secondary_log_colors`` argument can be used to create additional
        ``log_color`` attributes. Each key in the dictionary will set
        ``{key}_log_color``, using the value to select from a different
        ``log_colors`` set.

        :Parameters:
        - fmt (str): The format string to use
        - datefmt (str): A format string for the date
        - log_colors (dict):
            A mapping of log level names to color names
        - reset (bool):
            Implicitly append a color reset to all records unless False
        - style ('%' or '{' or '$'):
            The format style to use. (*No meaning prior to Python 3.2.*)
        - secondary_log_colors (dict):
            Map secondary ``log_color`` attributes. (*New in version 2.6.*)
        """
        if fmt is None:
            fmt = default_formats[style]

        if sys.version_info >= (3, 8):
            # Disable the new validation feature in 3.8, as out dictionary
            # of formats will cause it to error. All this logging needs refactoring
            # anyway.
            extra = dict(validate=False)
        else:
            extra = dict()

        super(LightbusFormatter, self).__init__(fmt, datefmt, style, **extra)

        # Disable reset codes if we do not have a TTY
        self.stream = stream or sys.stdout
        reset = reset and self.stream.isatty()

        self.log_colors = log_colors if log_colors is not None else default_log_colors
        self.secondary_log_colors = secondary_log_colors
        self.reset = reset
        self.style = style
        self.fmt = fmt

    def color(self, log_colors, level_name):
        """Return escape codes from a ``log_colors`` dict."""
        # Don't color log records if do not have a TTY
        if not self.stream.isatty():
            log_colors = {}
        return parse_colors(log_colors.get(level_name, ""))

    def format(self, record):
        """Format a message from a record object."""
        record = LightbusLogRecord(record)
        record.log_color = self.color(self.log_colors, record.levelname)
        record.is_tty = self.stream.isatty()

        # Set secondary log colors
        if self.secondary_log_colors:
            for name, log_colors in self.secondary_log_colors.items():
                color = self.color(log_colors, record.levelname)
                setattr(record, name + "_log_color", color)

        # Set format for this particular log level
        if isinstance(self.fmt, dict):
            self._fmt = self.fmt[record.levelname]
            # Update self._style because we've changed self._fmt
            # (code based on stdlib's logging.Formatter.__init__())
            if self.style not in logging._STYLES:
                raise ValueError("Style must be one of: %s" % ",".join(logging._STYLES.keys()))
            self._style = logging._STYLES[self.style][0](self._fmt)

        # Format the message
        record.additional_line_prefix = self.get_additional_line_prefix(record)
        message = super(LightbusFormatter, self).format(record)

        # Add a reset code to the end of the message
        # (if it wasn't explicitly added in format str)
        if self.reset and not message.endswith(escape_codes["reset"]):
            message += escape_codes["reset"]

        return message

    def get_additional_line_prefix(self, record):
        fmt_before_msg = self._fmt.split("%(msg)s", 1)
        if len(fmt_before_msg) == 1:
            return 0
        formatted_prefix = fmt_before_msg[0] % {
            k: "" if "color" in k else " " * len(str(v)) for k, v in record.__dict__.items()
        }
        return formatted_prefix


class L:  # pragma: no cover
    style = ""

    def __init__(self, log_message, *values):
        self.log_message = log_message
        self.values = values

    def __str__(self):
        return self.render()

    def __repr__(self):
        return repr(self.__str__())

    def render(self, parent_style="", style="", tty=True, additional_line_prefix="", msg_cb=None):
        style = style or self.style
        keys = [
            v.render(parent_style=style, tty=tty) if hasattr(v, "render") else v
            for v in self.values
        ]
        if tty:
            msg = str(self.log_message)
            if keys:
                msg = str(self.log_message).format(*keys)
            if msg_cb:
                msg = msg_cb(msg, is_tty=tty)
            return style + msg + escape_codes["reset"] + parent_style
        else:
            return str(self.log_message).format(*keys)


class Bold(L):  # pragma: no cover
    style = escape_codes["bold"]


class LBullets(L):  # pragma: no cover
    def __init__(self, log_message, *values, items, bullet="∙", indent=4):
        super().__init__(log_message, *values)
        self.items = items
        self.bullet = bullet
        self.indent = indent

    def render(self, parent_style="", style="", tty=True, additional_line_prefix="", msg_cb=None):
        if not tty:
            if isinstance(self.items, dict):
                return "{}: {}".format(
                    self.log_message, ", ".join(f"{k}:{v}" for k, v in self.items.items())
                )
            else:
                return "{}: {}".format(self.log_message, ", ".join(map(str, self.items)))

        style = style or self.style

        def render_child(item) -> str:
            if hasattr(item, "render"):
                return item.render(parent_style=style, tty=tty)
            else:
                return item

        rendered_items = []
        if isinstance(self.items, dict):
            key_width = max(map(len, self.items.keys())) + 1
            for k, v in self.items.items():
                rendered_items.append(
                    "{}: {}".format(render_child(k).ljust(key_width), render_child(v))
                )
        else:
            for item in self.items:
                rendered_items.append(render_child(item))

        indent = self.indent
        msg = "\n".join(
            ["{}:".format(self.log_message)]
            + [
                "{}{}{} {}".format(additional_line_prefix, " " * indent, self.bullet, item)
                for item in rendered_items
            ]
            + [additional_line_prefix]
        )
        if msg_cb:
            msg = msg_cb(msg, is_tty=tty)
        return style + msg + escape_codes["reset"] + parent_style
