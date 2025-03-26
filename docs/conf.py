"""Sphinx configuration."""
project = "HamContestLog"
author = "Roger Caminal Armadans, EA3M"
copyright = "2025, Roger Caminal Armadans, EA3M"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
