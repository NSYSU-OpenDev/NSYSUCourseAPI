"""Root conftest so pytest puts the repo root on sys.path.

Without this, `import utils.page_validation` fails when tests run from
the repository root.
"""
