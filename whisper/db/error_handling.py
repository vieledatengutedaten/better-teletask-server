from functools import wraps
import inspect
from typing import Any, Callable, ParamSpec, TypeVar

import logging

logger = logging.getLogger("btt_root_logger")

P = ParamSpec("P")
R = TypeVar("R")
# either string or function that receives args, kwargs, result and returns a string
SuccessMessage = str | Callable[[tuple[Any, ...], dict[str, Any], Any], str]


def db_operation(
    success_message: SuccessMessage | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Wrap database functions with consistent success/failure logging.

    - On success: logs `success_message` if provided.
            If string, named placeholders are supported from function arguments
            (e.g. "Added key {api_key}") plus `result` and `<arg>_len` helpers.
            If callable, it receives `(args, kwargs, result)`.
    - On failure: logs module/function and re-raises the original exception.
    """

    def decorator(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                result = function(*args, **kwargs)
                if success_message is not None:
                    message = (
                        success_message(args, kwargs, result)
                        if callable(success_message)
                        else _format_success_message(
                            function, args, kwargs, result, success_message
                        )
                    )
                    logger.info(message)
                return result
            except Exception:
                logger.exception(
                    "Database function %s.%s failed",
                    function.__module__,
                    function.__name__,
                )
                raise

        return wrapper

    return decorator


def _format_success_message(
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    result: Any,
    template: str,
) -> str:
    bound = inspect.signature(function).bind_partial(*args, **kwargs)
    context: dict[str, Any] = dict(bound.arguments)
    context["result"] = result

    for name, value in list(context.items()):
        if name == "result":
            continue
        try:
            context[f"{name}_len"] = len(value)
        except Exception:
            pass

    try:
        return template.format_map(context)
    except Exception:
        return template
