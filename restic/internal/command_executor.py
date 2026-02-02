import logging
import selectors
import subprocess
from collections import deque
from dataclasses import dataclass
from typing import Callable
from typing import Deque

import restic.errors

logger = logging.getLogger(__name__)


@dataclass
class StreamState:
    stream: bool = False
    on_line: Callable[[str], None] | None = None
    stdout_buffer: Deque[str] = deque()
    stderr_buffer: Deque[str] = deque()


def get_stdout_result(stdout_buffer, stream):
    if not stdout_buffer:
        return ""

    if stream:
        for line in reversed(stdout_buffer):
            if line.strip():
                return line
        return ""

    return "\n".join(stdout_buffer)


def handle_line(source, line, state):

    if state.stream and state.on_line:
        try:
            state.on_line(line)
        except Exception as e:  # pylint: disable=W0703
            logger.exception("Exception in on_line callback (ignored): %s", e)

    if source == "stdout":
        state.stdout_buffer.append(line)
    else:
        state.stderr_buffer.append(line)


def safe_drain(pipe, source, state) -> None:
    try:
        if pipe and not pipe.closed:
            for line in pipe:
                handle_line(source, line.rstrip(), state)
    except ValueError:
        # Pipe already closed – safe to ignore
        pass


def execute(cmd, *, stream=False, on_line=None, buffer_limit=1000):
    """
    Execute a restic command and return its stdout output.
    """
    logger.debug("Executing restic command: %s", cmd)

    try:
        with subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                encoding="utf-8",
        ) as process:

            # Use selectors to read stdout and stderr concurrently.
            selector = selectors.DefaultSelector()
            if process.stdout:
                selector.register(process.stdout, selectors.EVENT_READ,
                                  "stdout")
            if process.stderr:
                selector.register(process.stderr, selectors.EVENT_READ,
                                  "stderr")

            stdout_buffer = deque(maxlen=buffer_limit)
            stderr_buffer = deque(maxlen=buffer_limit)

            state = StreamState(
                stream,
                on_line,
                stdout_buffer,
                stderr_buffer,
            )

            try:
                while selector.get_map():

                    # Wait briefly for any stream to be ready.
                    for key, _ in selector.select(timeout=0.1):
                        stream_obj = key.fileobj
                        source = key.data

                        line = stream_obj.readline()
                        if not line:
                            # EOF reached for this stream, unregister and close.
                            selector.unregister(stream_obj)
                            continue

                        handle_line(source, line.rstrip(), state)

                    if process.poll() is not None and not selector.get_map():
                        break

            finally:
                selector.close()

            returncode = process.wait()

            # Final drain to catch last buffered lines (e.g. restic summary)
            safe_drain(process.stdout, "stdout", state)
            safe_drain(process.stderr, "stderr", state)

    except FileNotFoundError as e:
        raise restic.errors.NoResticBinaryError(
            "Cannot find restic installed") from e

    logger.debug("Restic command completed with return code %d", returncode)

    if returncode != 0:
        stderr_text = "\n".join(stderr_buffer)
        raise restic.errors.ResticFailedError(
            f"Restic failed with exit code {returncode}: {stderr_text}")

    result = get_stdout_result(stdout_buffer, stream)
    return result
