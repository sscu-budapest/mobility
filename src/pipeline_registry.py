import inspect
import os
import random
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Union

import numpy as np
from invoke import task
from parquetranger import TableRepo


class PipelineRegistry:
    def __init__(self):
        self._steps = {}

    def register(
        self,
        procfun=None,
        *,
        dependencies: Optional[list] = None,
        outputs: Optional[list] = None,
        outputs_nocache: Optional[list] = None,
    ):
        """the names of parameters will matter and will be looked up in params.yaml"""

        def f(fun):
            fs = traceback.extract_stack()[-2]
            relpath = os.path.relpath(fs.filename, os.getcwd())
            lineno = fs.lineno
            parsed_deps = self._parse_list(dependencies)

            pe = PipelineElement(
                runner=fun,
                outputs=self._parse_list(outputs),
                param_list=inspect.getfullargspec(fun).args,
                dependencies=[relpath] + parsed_deps,
                out_nocache=self._parse_list(outputs_nocache),
                lineno=lineno,
            )
            self._steps[pe.name] = pe
            return fun

        if procfun is None:
            return f
        return f(procfun)

    def get_step(self, name: str) -> "PipelineElement":
        return self._steps[name]

    @property
    def steps(self) -> Iterable["PipelineElement"]:
        return self._steps.values()

    def _parse_elem(
        self,
        elem: Union[
            str,
        ],
    ) -> str:
        if isinstance(elem, str):
            return [elem]
        if isinstance(elem, Path):
            return [str(elem)]
        if isinstance(elem, TableRepo):
            return [elem.full_path]
        if isinstance(elem, type):
            return [os.path.relpath(inspect.getfile(elem), os.getcwd())]
        if callable(elem):
            pe = self._steps[elem.__name__]
            return pe.outputs + pe.out_nocache
        raise TypeError(f"{type(elem)} was given as parameter for pipeline element")

    def _parse_list(self, elemlist):
        return sum([self._parse_elem(e) for e in elemlist or []], [])


@dataclass
class PipelineElement:

    runner: callable
    outputs: list
    param_list: list
    dependencies: list
    out_nocache: list
    lineno: int

    def run(self, loaded_params: dict):

        parsed_params = {}
        _level_params = loaded_params.get(self.name, {})
        for k in self.param_list:
            if k == "seed":
                v = loaded_params[k]
                np.random.seed(v)
                random.seed(v)
            else:
                parsed_params[k] = _level_params[k]

        self.runner(**parsed_params)

    def get_invoke_task(self):
        param_str = ",".join(
            [(".".join([self.name, p]) if p != "seed" else p) for p in self.param_list]
        )
        if param_str:
            param_str = "-p " + param_str
        command = " ".join(
            [
                f"dvc run -n {self.name} --force",
                param_str,
                _get_comm(self.dependencies, "d"),
                _get_comm(self.outputs, "o"),
                _get_comm(self.out_nocache, "O"),
                f"python -m src {self.name}",
            ]
        )

        @task(name=self.name)
        def _task(c):
            c.run(command)

        return _task

    def get_dag_replace(self, link_base):
        link = link_base.format(self.dependencies[0], self.lineno)
        return self.name, f"[{self.name}]({link})"

    @property
    def name(self):
        return self.runner.__name__


def _get_comm(entries, prefix):
    return " ".join([f"-{prefix} {e}" for e in entries])


pipereg = PipelineRegistry()
