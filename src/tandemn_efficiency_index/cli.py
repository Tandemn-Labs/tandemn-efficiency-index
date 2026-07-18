"""Command-line control plane for Tandemn Efficiency Index."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import socket
import subprocess
import sys
import time
import webbrowser
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any

from tandemn_efficiency_index.control import ControlPlaneError, TeiControlClient

DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_NAMESPACE = "tei-system"
DEFAULT_RELEASE = "tei"


@dataclass(frozen=True)
class Connection:
    """One resolved connection to a TEI control plane."""

    client: TeiControlClient
    dashboard_url: str
    port_forward: KubernetesPortForward | None = None


class KubernetesPortForward:
    """Own a temporary kubectl port-forward to the TEI Service."""

    def __init__(
        self,
        namespace: str,
        service: str,
        local_port: int,
        remote_port: int,
        timeout_seconds: float,
    ) -> None:
        self.namespace = namespace
        self.service = service
        self.local_port = local_port or _available_port()
        self.remote_port = remote_port
        self.timeout_seconds = timeout_seconds
        self._process: subprocess.Popen[bytes] | None = None

    @property
    def url(self) -> str:
        """Return the local URL exposed by the port-forward."""
        return f"http://127.0.0.1:{self.local_port}"

    def start(self) -> None:
        """Start kubectl and wait for its local socket to accept connections."""
        command = [
            "kubectl",
            "--namespace",
            self.namespace,
            "port-forward",
            f"service/{self.service}",
            f"{self.local_port}:{self.remote_port}",
        ]
        try:
            self._process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise ControlPlaneError("kubectl is required for --kube connections") from exc

        deadline = time.monotonic() + self.timeout_seconds
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                message = _process_error(self._process)
                raise ControlPlaneError(
                    f"kubectl port-forward exited before becoming ready: {message}"
                )
            try:
                with socket.create_connection(
                    ("127.0.0.1", self.local_port),
                    timeout=0.2,
                ):
                    return
            except OSError:
                time.sleep(0.1)
        self.stop()
        raise ControlPlaneError("Timed out waiting for the Kubernetes port-forward")

    def wait(self) -> None:
        """Wait until the port-forward exits."""
        if self._process is not None:
            self._process.wait()

    def stop(self) -> None:
        """Terminate the owned port-forward process."""
        if self._process is None or self._process.poll() is not None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the TEI command-line control plane."""
    arguments = list(argv if argv is not None else sys.argv[1:])
    arguments = _expand_root_aliases(arguments)
    parser = _parser()
    args = parser.parse_args(arguments)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 0
    try:
        return int(args.handler(args))
    except ControlPlaneError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return _error_exit_code(exc)
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)
        return 130


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tei",
        description="Control Tandemn Efficiency Index observability.",
    )
    parser.add_argument("--version", action="version", version=version("tandemn-efficiency-index"))
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    server = subparsers.add_parser("server", help="Run the in-cluster TEI service")
    server.set_defaults(handler=_run_server)

    status = subparsers.add_parser("status", help="Show control-plane and dependency status")
    _add_connection_options(status)
    _add_json_option(status)
    status.set_defaults(handler=_status)

    health = subparsers.add_parser("health", help="Check service liveness")
    _add_connection_options(health)
    _add_json_option(health)
    health.set_defaults(handler=_health)

    ready = subparsers.add_parser("ready", help="Check collection readiness")
    _add_connection_options(ready)
    _add_json_option(ready)
    ready.set_defaults(handler=_ready)

    snapshot = subparsers.add_parser("snapshot", help="Fetch an observability snapshot")
    _add_connection_options(snapshot)
    snapshot.add_argument("--window", default="1h", help="15m, 1h, 6h, 24h, or all")
    snapshot.add_argument("--max-points", type=int, default=180)
    snapshot.add_argument("--output", type=Path, help="Write JSON to a file")
    snapshot.set_defaults(handler=_snapshot)

    workloads = subparsers.add_parser("workloads", help="List observed workloads")
    _add_connection_options(workloads)
    _add_json_option(workloads)
    workloads.set_defaults(handler=_workloads)

    observe = subparsers.add_parser("observe", help="Control periodic observation collection")
    observe_subparsers = observe.add_subparsers(dest="observe_command", metavar="ACTION")
    for action, help_text in (
        ("start", "Start or resume observation collection"),
        ("stop", "Stop collection while keeping the API available"),
        ("restart", "Restart observation collection"),
    ):
        command = observe_subparsers.add_parser(action, help=help_text)
        _add_connection_options(command)
        _add_json_option(command)
        command.set_defaults(handler=_observation_action, observation_action=action)

    dashboard = subparsers.add_parser("dashboard", help="Open the TEI dashboard")
    _add_connection_options(dashboard)
    dashboard.add_argument("--no-open", action="store_true", help="Print the URL only")
    dashboard.set_defaults(handler=_dashboard)

    shell = subparsers.add_parser("shell", help="Open the interactive slash-command shell")
    _add_connection_options(shell)
    shell.set_defaults(handler=_shell)

    logs = subparsers.add_parser("logs", help="Read logs from the Kubernetes TEI deployment")
    logs.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    logs.add_argument("--release", default=DEFAULT_RELEASE)
    logs.add_argument("--deployment", help="Override the Kubernetes TEI Deployment name")
    logs.add_argument("--follow", "-f", action="store_true")
    logs.add_argument("--tail", type=int, default=200)
    logs.set_defaults(handler=_logs)
    return parser


def _add_connection_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--api-url",
        default=os.environ.get("TEI_API_URL", DEFAULT_API_URL),
        help="Direct TEI URL; defaults to TEI_API_URL or %(default)s",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("TEI_API_TOKEN") or os.environ.get("TEI_API_BEARER_TOKEN"),
        help="API bearer token; defaults to TEI_API_TOKEN",
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--kube",
        action="store_true",
        help="Connect through a temporary Kubernetes port-forward",
    )
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--release", default=DEFAULT_RELEASE)
    parser.add_argument("--service", help="Override the Kubernetes TEI Service name")
    parser.add_argument("--local-port", type=int, default=0)


def _add_json_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")


@contextmanager
def _connection(args: argparse.Namespace) -> Iterator[Connection]:
    port_forward: KubernetesPortForward | None = None
    api_url = str(args.api_url)
    if args.kube:
        port_forward = KubernetesPortForward(
            namespace=str(args.namespace),
            service=str(args.service or f"{args.release}-tei"),
            local_port=int(args.local_port),
            remote_port=8000,
            timeout_seconds=float(args.timeout),
        )
        port_forward.start()
        api_url = port_forward.url
    try:
        yield Connection(
            client=TeiControlClient(api_url, args.token, args.timeout),
            dashboard_url=api_url,
            port_forward=port_forward,
        )
    finally:
        if port_forward is not None:
            port_forward.stop()


def _run_server(args: argparse.Namespace) -> int:
    from tandemn_efficiency_index.app import main as server_main

    server_main()
    return 0


def _status(args: argparse.Namespace) -> int:
    with _connection(args) as connection:
        payload = connection.client.status()
    _render_status(payload, args.json)
    return 0 if payload.get("ready") else 4


def _health(args: argparse.Namespace) -> int:
    with _connection(args) as connection:
        payload = connection.client.health()
    _render_simple_status("Health", payload, args.json)
    return 0 if payload.get("healthy") else 3


def _ready(args: argparse.Namespace) -> int:
    with _connection(args) as connection:
        payload = connection.client.readiness()
    _render_simple_status("Readiness", payload, args.json)
    return 0 if payload.get("ready") else 4


def _snapshot(args: argparse.Namespace) -> int:
    window_seconds = _window_seconds(args.window)
    with _connection(args) as connection:
        payload = connection.client.snapshot(window_seconds, args.max_points)
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote snapshot to {args.output}")
    else:
        print(rendered)
    return 0


def _workloads(args: argparse.Namespace) -> int:
    with _connection(args) as connection:
        payload = connection.client.snapshot(3600, 2)
    workloads = [
        {
            "workload_id": job.get("workload_id"),
            "active": job.get("active"),
            "runtime": job.get("workload", {}).get("runtime"),
            "namespace": job.get("workload", {}).get("namespace"),
            "name": job.get("workload", {}).get("name"),
            "model_id": job.get("workload", {}).get("model_id"),
            "backend": job.get("workload", {}).get("backend"),
            "workers": len(job.get("workers", [])),
            "coverage": job.get("coverage", {}).get("status"),
        }
        for job in payload.get("jobs", [])
    ]
    if args.json:
        _print_json({"workloads": workloads})
        return 0
    if not workloads:
        print("No workloads observed.")
        return 0
    print("RUNTIME  NAMESPACE  NAME  MODEL  BACKEND  WORKERS  COVERAGE  STATE")
    for workload in workloads:
        state = "active" if workload["active"] else "inactive"
        print(
            f"{workload['runtime']}  {workload['namespace']}  {workload['name']}  "
            f"{workload['model_id']}  {workload['backend']}  {workload['workers']}  "
            f"{workload['coverage']}  {state}"
        )
    return 0


def _observation_action(args: argparse.Namespace) -> int:
    with _connection(args) as connection:
        actions = {
            "start": connection.client.start_observation,
            "stop": connection.client.stop_observation,
            "restart": connection.client.restart_observation,
        }
        payload = actions[args.observation_action]()
    _render_status(payload, args.json)
    return 0


def _dashboard(args: argparse.Namespace) -> int:
    with _connection(args) as connection:
        print(connection.dashboard_url)
        if not args.no_open:
            webbrowser.open(connection.dashboard_url)
        if connection.port_forward is not None:
            print("Kubernetes port-forward active; press Ctrl+C to close it.")
            connection.port_forward.wait()
    return 0


def _shell(args: argparse.Namespace) -> int:
    with _connection(args) as connection:
        print(f"Connected to TEI at {connection.dashboard_url}")
        print("Type /help for commands and /quit to exit.")
        while True:
            try:
                line = input("tei> ").strip()
            except EOFError:
                print()
                return 0
            if not line:
                continue
            if not line.startswith("/"):
                print("Slash commands begin with /. Type /help for available commands.")
                continue
            try:
                tokens = shlex.split(line[1:])
                if _run_shell_command(tokens, connection):
                    return 0
            except (ControlPlaneError, ValueError) as exc:
                print(f"Error: {exc}", file=sys.stderr)


def _run_shell_command(tokens: list[str], connection: Connection) -> bool:
    if not tokens:
        return False
    command = tokens[0].lower()
    if command in {"quit", "exit"}:
        return True
    if command == "help":
        print(
            "/status  /health  /ready  /workloads  /snapshot [window]  "
            "/start  /stop  /restart  /dashboard  /help  /quit"
        )
        return False
    if command == "status":
        _render_status(connection.client.status(), False)
    elif command == "health":
        _render_simple_status("Health", connection.client.health(), False)
    elif command == "ready":
        _render_simple_status("Readiness", connection.client.readiness(), False)
    elif command == "workloads":
        payload = connection.client.snapshot(3600, 2)
        for job in payload.get("jobs", []):
            workload = job.get("workload", {})
            print(
                f"{workload.get('runtime')}  {workload.get('namespace')}/"
                f"{workload.get('name')}  {workload.get('model_id')}"
            )
    elif command == "snapshot":
        selected_window = tokens[1] if len(tokens) > 1 else "1h"
        _print_json(connection.client.snapshot(_window_seconds(selected_window), 180))
    elif command == "start":
        _render_status(connection.client.start_observation(), False)
    elif command == "stop":
        _render_status(connection.client.stop_observation(), False)
    elif command == "restart":
        _render_status(connection.client.restart_observation(), False)
    elif command == "dashboard":
        print(connection.dashboard_url)
        webbrowser.open(connection.dashboard_url)
    else:
        raise ValueError(f"Unknown slash command: /{command}")
    return False


def _logs(args: argparse.Namespace) -> int:
    deployment = args.deployment or f"{args.release}-tei"
    command = [
        "kubectl",
        "--namespace",
        args.namespace,
        "logs",
        f"deployment/{deployment}",
        f"--tail={args.tail}",
    ]
    if args.follow:
        command.append("--follow")
    try:
        return subprocess.run(command, check=False).returncode
    except FileNotFoundError as exc:
        raise ControlPlaneError("kubectl is required to read Kubernetes logs") from exc


def _render_status(payload: dict[str, Any], as_json: bool) -> None:
    if as_json:
        _print_json(payload)
        return
    collector = payload.get("collector") or {}
    storage = collector.get("storage") or {}
    print(f"Lifecycle    {payload.get('lifecycle', 'unknown')}")
    print(f"Ready        {_yes_no(payload.get('ready'))}")
    print(f"Last tick    {payload.get('last_tick_completed_at') or 'never'}")
    print(f"Prometheus   {_yes_no(collector.get('last_prometheus_check_at'))}")
    print(f"PostgreSQL   {_yes_no(storage.get('writable'))}")
    error = payload.get("last_transition_error") or collector.get("last_collection_error")
    if error:
        print(f"Error        {error}")


def _render_simple_status(label: str, payload: dict[str, Any], as_json: bool) -> None:
    if as_json:
        _print_json(payload)
        return
    state = payload.get("healthy", payload.get("ready", False))
    print(f"{label}: {'ok' if state else 'not ready'}")
    print(f"Lifecycle: {payload.get('lifecycle', 'unknown')}")


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _window_seconds(value: str) -> int:
    normalized = value.strip().lower()
    windows = {
        "15m": 15 * 60,
        "1h": 60 * 60,
        "6h": 6 * 60 * 60,
        "24h": 24 * 60 * 60,
        "all": 0,
    }
    try:
        return windows[normalized]
    except KeyError as exc:
        raise ValueError("window must be one of: 15m, 1h, 6h, 24h, all") from exc


def _available_port() -> int:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def _process_error(process: subprocess.Popen[bytes]) -> str:
    if process.stderr is None:
        return f"exit code {process.returncode}"
    message = process.stderr.read().decode(errors="replace").strip()
    return message or f"exit code {process.returncode}"


def _yes_no(value: Any) -> str:
    return "yes" if bool(value) else "no"


def _error_exit_code(error: ControlPlaneError) -> int:
    if error.status_code in {401, 403}:
        return 5
    if error.status_code is not None:
        return 3
    return 1


def _expand_root_aliases(arguments: list[str]) -> list[str]:
    aliases = {
        "--status": "status",
        "--dashboard": "dashboard",
        "--start": "observe start",
        "--stop": "observe stop",
        "--restart": "observe restart",
    }
    if arguments and arguments[0] in aliases:
        return shlex.split(aliases[arguments[0]]) + arguments[1:]
    return arguments
