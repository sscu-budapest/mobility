from sscutils import project_ns as ns

try:
    from src import pipereg
    ns.add_collection(pipereg.get_collection())
except ImportError:
    pass