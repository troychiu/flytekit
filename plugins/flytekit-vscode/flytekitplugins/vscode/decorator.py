import importlib
import multiprocessing
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import time
from functools import wraps
from typing import Callable, Optional

import fsspec
from flytekit.core.context_manager import FlyteContextManager

from flytekit.loggers import logger

from .constants import (
    DEFAULT_CODE_SERVER_DIR_NAME,
    DEFAULT_CODE_SERVER_REMOTE_PATH,
    DEFAULT_UP_SECONDS,
    DOWNLOAD_DIR,
    EXECUTABLE_NAME,
)


def execute_command(cmd) -> None:
    """
    Execute a command in the shell.

    Args:
        cmd (str): The shell command to execute.
    """

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info(f"cmd: {cmd}")
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"Command {cmd} failed with error: {stderr}")
    logger.info(f"stdout: {stdout}")
    logger.info(f"stderr: {stderr}")


def download_file(url, target_dir=".") -> str:
    """
    Download a file from a given URL using fsspec.

    Args:
        url (str): The URL of the file to download.
        target_dir (str, optional): The directory where the file should be saved. Defaults to current directory.

    Returns:
        str: The path to the downloaded file.
    """

    if not url.startswith("http"):
        raise ValueError(f"URL {url} is not valid. Only http/https is supported.")

    # Derive the local filename from the URL
    local_file_name = os.path.join(target_dir, os.path.basename(url))

    fs = fsspec.filesystem("http")

    # Use fsspec to get the remote file and save it locally
    logger.info(f"Downloading {url}... to {os.path.abspath(local_file_name)}")
    fs.get(url, local_file_name)
    logger.info("File downloaded successfully!")

    return local_file_name


def download_vscode(
    code_server_remote_path: str,
    code_server_dir_name: str,
) -> None:
    """
    Download vscode server and plugins from remote to local and add the directory of binary executable to $PATH.

    Args:
        code_server_remote_path (str): The URL of the code-server tarball.
        code_server_dir_name (str): The name of the code-server directory.
    """

    # If the code server already exists in the container, skip downloading
    executable_path = shutil.which(EXECUTABLE_NAME)
    if executable_path is not None:
        logger.info(f"Code server binary already exists at {executable_path}")
        logger.info("Skipping downloading code server...")
        return

    logger.info("Code server is not in $PATH, start downloading code server...")

    # Create DOWNLOAD_DIR if not exist
    logger.info(f"DOWNLOAD_DIR: {DOWNLOAD_DIR}")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    logger.info(f"Start downloading files to {DOWNLOAD_DIR}")

    # Download remote file to local
    code_server_tar_path = download_file(code_server_remote_path, DOWNLOAD_DIR)

    # Extract the tarball
    with tarfile.open(code_server_tar_path, "r:gz") as tar:
        tar.extractall(path=DOWNLOAD_DIR)

    code_server_dir_path = os.path.join(DOWNLOAD_DIR, code_server_dir_name)

    code_server_bin_dir = os.path.join(code_server_dir_path, "bin")

    # Add the directory of code-server binary to $PATH
    os.environ["PATH"] = code_server_bin_dir + os.pathsep + os.environ["PATH"]


def generate_back_to_batch_job_script() -> None:
    file_name = "back_to_batch_job"
    back_to_batch_job_sh = f"""#!/bin/bash
echo "Terminating server and will back to batch job. Goodbye!"
PID={os.getpid()}
kill -TERM $PID
"""

    with open(file_name, "w") as file:
        file.write(back_to_batch_job_sh)
    os.chmod(file_name, 0o755)
    logger.info(f"current working dir: {os.getcwd()}")
    os.environ["PATH"] = os.getcwd() + os.pathsep + os.environ["PATH"]
    logger.info(f'PATH: {os.environ["PATH"]}')

def back_to_batch_job_handler(signum, frame):
    """
    TODO.

    Args:
        signum (): 
        frame (): 
    """
    global back_to_batch_job
    back_to_batch_job = True
    return

def load_module_from_path(module_name, path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is not None:
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    else:
        raise ImportError(f"Module at {path} could not be loaded")

back_to_batch_job = False
def vscode(
    _task_function: Optional[Callable] = None,
    server_up_seconds: Optional[int] = DEFAULT_UP_SECONDS,
    port: Optional[int] = 8080,
    enable: Optional[bool] = True,
    code_server_remote_path: Optional[str] = DEFAULT_CODE_SERVER_REMOTE_PATH,
    # The untarred directory name may be different from the tarball name
    code_server_dir_name: Optional[str] = DEFAULT_CODE_SERVER_DIR_NAME,
    pre_execute: Optional[Callable] = None,
    post_execute: Optional[Callable] = None,
    run_task_first=False,
) -> Callable:
    """
    vscode decorator modifies a container to run a VSCode server:
    1. Overrides the user function with a VSCode setup function.
    2. Download vscode server and plugins from remote to local.
    3. Launches and monitors the VSCode server.
    4. Terminates after server_up_seconds seconds.

    Args:
        _task_function (function, optional): The user function to be decorated. Defaults to None.
        port (int, optional): The port to be used by the VSCode server. Defaults to 8080.
        enable (bool, optional): Whether to enable the VSCode decorator. Defaults to True.
        code_server_remote_path (str, optional): The URL of the code-server tarball.
        code_server_dir_name (str, optional): The name of the code-server directory.
        pre_execute (function, optional): The function to be executed before the vscode setup function.
        post_execute (function, optional): The function to be executed before the vscode is self-terminated.
    """

    def wrapper(fn: Callable) -> Callable:
        if not enable:
            return fn

        @wraps(fn)
        def inner_wrapper(*args, **kwargs):
            ctx = FlyteContextManager.current_context()
            if ctx.execution_state.is_local_execution():
                return fn(*args, **kwargs)
            if run_task_first:
                logger.info("Run task")
                try:
                    res = fn(*args, **kwargs)
                    return res
                except Exception as e:
                    logger.info(f"task error: {e}")
                    logger.info("Start hosting code server")
    
            shutil.copy(f"./{fn.__module__}.py", os.path.join(ctx.execution_state.working_dir, f"{fn.__module__}.py"))
            generate_interactive_python(fn)

            # 0. Executes the pre_execute function if provided.
            if pre_execute is not None:
                pre_execute()
                logger.info("Pre execute function executed successfully!")

            # 1. Downloads the VSCode server from Internet to local.
            download_vscode(
                code_server_remote_path=code_server_remote_path,
                code_server_dir_name=code_server_dir_name,
            )

            # 2. Launches and monitors the VSCode server.
            # Run the function in the background
            logger.info(f"Start the server for {server_up_seconds} seconds...")
            code_server_process = multiprocessing.Process(
                target=execute_command, kwargs={"cmd": f"code-server --bind-addr 0.0.0.0:{port} --auth none"}
            )
            code_server_process.start()

            # 3. generate tasks.json and register signal handler for back to batch job
            logger.info("Generate tasks.json for back to batch job")
            generate_back_to_batch_job_script()

            logger.info("Register signal handler for backing to batch job")
            signal.signal(signal.SIGTERM, back_to_batch_job_handler)

            # 4. Terminates the server after server_up_seconds
            sleep_interval = 0
            while sleep_interval < server_up_seconds and not back_to_batch_job:
                sleep_time = 10
                time.sleep(sleep_time)
                sleep_interval += sleep_time

            logger.info(f"{server_up_seconds} seconds passed. Terminating...")
            if post_execute is not None:
                post_execute()
                logger.info("Post execute function executed successfully!")
            code_server_process.terminate()
            code_server_process.join()

            if back_to_batch_job:
                logger.info("Back to batch job")
                task_function = getattr(load_module_from_path(fn.__module__, f"/root/{fn.__module__}.py"), fn.__name__)
                while hasattr(task_function, '__wrapped__'):
                    task_function = task_function.__wrapped__
                return task_function(*args, **kwargs)
            
            sys.exit(0)

        # inner_wrapper.__vscode__ = True
        return inner_wrapper

    # for the case when the decorator is used without arguments
    if _task_function is not None:
        return wrapper(_task_function)
    # for the case when the decorator is used with arguments
    else:
        return wrapper



def generate_interactive_python(user_function):
    task_module_name, task_name = user_function.__module__, user_function.__name__
    working_dir = FlyteContextManager.current_context().execution_state.working_dir
    # Create the file content
    file_content = f"""from {task_module_name} import {task_name}
from flytekit.core import utils
from flytekit.core.context_manager import FlyteContextManager
from flyteidl.core import literals_pb2 as _literals_pb2
from flytekit.core.type_engine import TypeEngine
from flytekit.models import literals as _literal_models
import sys
import importlib
import os

def load_module_from_path(module_name, path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is not None:
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    else:
        raise ImportError(f"Module could not be loaded")

def get_inputs():
    task_module_name, task_name = "{task_module_name}", "{task_name}"
    working_dir = "{working_dir}"
    ctx = FlyteContextManager()
    local_inputs_file = os.path.join(working_dir, "inputs.pb")
    input_proto = utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)
    idl_input_literals = _literal_models.LiteralMap.from_flyte_idl(input_proto)
    task_module = load_module_from_path(task_module_name, "{working_dir}/{task_module_name}.py")  # type: ignore
    task_def = getattr(task_module, task_name)
    native_inputs = TypeEngine.literal_map_to_kwargs(
        ctx, idl_input_literals, task_def.python_interface.inputs
    )
    return native_inputs


if __name__ == "__main__":
    inputs = get_inputs()
    print({task_name}(**inputs))
"""
    with open("interactive_debug.py", "w") as file:
        file.write(file_content)