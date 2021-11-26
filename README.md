# Research Project Template - WIP

> relies on [sscutils](https://github.com/sscu-budapest/sscutils) and the [tooling](https://sscu-budapest.github.io/tooling) of sscub

### To create a new project

- install requirements `pip install -r requirements`
- add datasets that use the [template](https://github.com/sscu-budapest/dataset-template) to `imported-namespaces.yaml`
- import them using `inv import-namespaces` and `inv load-external-data`
  - ensure that you import from branches that serve the data from sources you have access to
- create, register and document steps in a pipeline you will run on the data
  - initially change `src/step.py` and the related import in `src/__init__.py`
- run pipeline steps using invoke
  - check the available, registered steps using `inv -l`

### config files

- `defalt-remotes.yaml`
  - `branch: remote`
- `project-env.yaml` 
  - `local_name: {env: ..., tag: ...}`

### metadata

- `imported-namespaces.yaml`

## Test projects

TODO: document dogshow here