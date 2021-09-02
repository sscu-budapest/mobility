# Research Project Template - WIP

> relies on [sscutils](https://github.com/sscu-budapest/sscutils) and the [tooling](https://sscu-budapest.github.io/tooling) of sscub

### To create a new project

- install requirements `pip install -r requirements`
- add datasets that use the [template](https://github.com/sscu-budapest/dataset-template) to `datasets.yaml`
- import them using `inv import-data`
  - ensure that you import from branches that serve the data from sources you have access to
- create, register and document steps in a pipeline you will run on teh data
  - initially change `src/step.py` and the related import in `src/__init__.py`
- run pipeline steps using invoke
  - check the available, registered steps using `inv -l`

## Test projects

So far, only a small [one](https://github.com/sscu-budapest/test-project-a)

