import asyncio
from unittest.mock import MagicMock, patch

from webhook_delivery_service.api_runner import main


def test_api_runner_uses_selector_event_loop_on_windows() -> None:
    api_coroutine = MagicMock()
    serve_api_mock = MagicMock(return_value=api_coroutine)

    with (
        patch(
            "webhook_delivery_service.api_runner.sys.platform",
            "win32",
        ),
        patch(
            "webhook_delivery_service.api_runner.serve_api",
            serve_api_mock,
        ),
        patch(
            "webhook_delivery_service.api_runner.asyncio.run",
        ) as asyncio_run_mock,
    ):
        main()

    serve_api_mock.assert_called_once_with()
    asyncio_run_mock.assert_called_once_with(
        api_coroutine,
        loop_factory=asyncio.SelectorEventLoop,
    )


def test_api_runner_stops_cleanly_on_keyboard_interrupt() -> None:
    api_coroutine = MagicMock()
    serve_api_mock = MagicMock(return_value=api_coroutine)

    with (
        patch(
            "webhook_delivery_service.api_runner.serve_api",
            serve_api_mock,
        ),
        patch(
            "webhook_delivery_service.api_runner.asyncio.run",
            side_effect=KeyboardInterrupt,
        ) as asyncio_run_mock,
        patch("builtins.print") as print_mock,
    ):
        main()

    serve_api_mock.assert_called_once_with()
    asyncio_run_mock.assert_called_once()
    print_mock.assert_called_once_with("API stopped.")
