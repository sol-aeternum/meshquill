"""Immutable Meshquill Python hook bootstrap, executed with ``python -I -B -c``.

The Rust side bounds all input and output. This module deliberately reports only stable error
kinds; exception messages and tracebacks may contain message content or other secrets.
"""

import asyncio
import contextlib
import importlib.util
import inspect
import json
import pathlib
import sys


SCHEMA = "meshquill.hook/v1"
HANDLERS = (
    "on_connect",
    "on_disconnect",
    "on_message",
    "before_send",
    "after_send",
    "on_ack",
    "on_timeout",
    "on_contact_update",
    "on_error",
)


def _error(kind, handler=None):
    error = {"kind": kind}
    if handler in HANDLERS:
        error["handler"] = handler
    return {"schema": SCHEMA, "status": "error", "error": error}


def _valid_signature(value):
    try:
        parameters = list(inspect.signature(value).parameters.values())
    except (TypeError, ValueError):
        return False
    return len(parameters) == 1 and parameters[0].kind in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )


def _load_script(script):
    path = pathlib.Path(script)
    spec = importlib.util.spec_from_file_location("_meshquill_local_hook", path)
    if spec is None or spec.loader is None:
        raise ImportError
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
    except BaseException:
        raise
    return module


def _inspect_handlers(module, require_any):
    found = []
    for handler_name in HANDLERS:
        if not hasattr(module, handler_name):
            continue
        handler = getattr(module, handler_name)
        if not callable(handler):
            return None, _error("not_callable", handler_name)
        if not _valid_signature(handler):
            return None, _error("invalid_signature", handler_name)
        found.append(handler_name)
    if require_any and not found:
        return None, _error("no_handlers")
    return found, None


def _normalize_before_send(result):
    if result is None:
        return {"action": "allow"}
    if not isinstance(result, dict):
        raise ValueError
    action = result.get("action")
    if action == "allow":
        return {"action": "allow"}
    if action == "modify":
        normalized = {"action": "modify"}
        supplied = False
        for field in ("destination", "text"):
            if field in result:
                if not isinstance(result[field], str):
                    raise ValueError
                normalized[field] = result[field]
                supplied = True
        if not supplied:
            raise ValueError
        return normalized
    if action == "reject" and isinstance(result.get("reason"), str):
        return {"action": "reject", "reason": result["reason"]}
    raise ValueError


def _await_if_needed(value):
    if inspect.isawaitable(value):
        return asyncio.run(value)
    return value


def _run(request):
    if request.get("schema") != SCHEMA:
        return _error("invalid_request")
    operation = request.get("operation")
    script = request.get("script")
    if operation not in ("validate", "invoke") or not isinstance(script, str):
        return _error("invalid_request")
    try:
        module = _load_script(script)
    except BaseException:
        return _error("load_error")
    handlers, error = _inspect_handlers(module, operation == "validate")
    if error is not None:
        return error
    if operation == "validate":
        return {"schema": SCHEMA, "status": "validated", "handlers": handlers}

    envelope = request.get("envelope")
    if not isinstance(envelope, dict) or envelope.get("schema") != SCHEMA:
        return _error("invalid_request")
    handler_name = envelope.get("event")
    if handler_name not in HANDLERS:
        return _error("invalid_request")
    if handler_name not in handlers:
        return {"schema": SCHEMA, "status": "missing"}
    try:
        result = _await_if_needed(getattr(module, handler_name)(envelope))
    except BaseException:
        return _error("hook_exception", handler_name)
    if handler_name == "before_send":
        try:
            result = _normalize_before_send(result)
        except BaseException:
            return _error("invalid_result", handler_name)
        return {"schema": SCHEMA, "status": "invoked", "result": result}
    return {"schema": SCHEMA, "status": "invoked"}


def _main():
    protocol_stdout = sys.stdout
    diagnostics = sys.stderr
    try:
        request = json.load(sys.stdin)
    except BaseException:
        response = _error("invalid_request")
    else:
        with contextlib.redirect_stdout(diagnostics), contextlib.redirect_stderr(diagnostics):
            try:
                response = _run(request)
            except BaseException:
                response = _error("internal_error")
    try:
        json.dump(response, protocol_stdout, ensure_ascii=False, separators=(",", ":"))
        protocol_stdout.write("\n")
        protocol_stdout.flush()
    except BaseException:
        pass


sys.dont_write_bytecode = True
_main()
