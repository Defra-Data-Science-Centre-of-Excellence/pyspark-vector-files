"""Nox sessions."""
from os import environ
from pathlib import Path
from shutil import copytree, rmtree

import nox
from nox_poetry import Session, session

package = "pyspark_vector_files"
nox.options.sessions = "isort", "black", "lint", "safety", "mypy"  # , "tests"
locations = "src", "tests", "noxfile.py"
# locations = "src", "tests", "noxfile.py", "docs/source/conf.py"


@session(python="3.8")
def isort(session: Session) -> None:
    """Sort imports with isort."""
    args = session.posargs or locations
    session.install("isort")
    session.run("isort", *args)


@session(python="3.8")
def black(session: Session) -> None:
    """Run black code formatter."""
    args = session.posargs or locations
    session.install("black")
    session.run("black", *args)


@session(python="3.8")
def lint(session: Session) -> None:
    """Lint using flake8."""
    args = session.posargs or locations
    deps = [
        "flake8",
        "flake8-annotations",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-isort",
        "darglint",
    ]
    session.install(*deps)
    session.run("flake8", *args)


@session(python="3.8")
def safety(session: Session) -> None:
    """Scan dependencies for insecure packages."""
    requirements = session.poetry.export_requirements()
    session.install("safety")
    session.run(
        "safety",
        "check",
        "--full-report",
        f"--file={requirements}",
        # ! Ignoring GDAL vulnerability as v3.4.3 is the latest version
        # ! available through ubuntugis-unstable (EFT, 2022-08-10)
        "--ignore=48545",
    )


@session(python="3.8")
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or locations
    deps = [".", "mypy", "pytest"]
    session.install(*deps)
    session.run("mypy", *args, "--ignore-missing-imports")


@session(python="3.8")
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or [
        "--cov",
        "-vv",
    ]
    session.install(".")
    session.install(
        "coverage[toml]",
        "pytest",
        "pytest-cov",
        "geopandas",
        "chispa",
        "pyarrow",
    )
    session.run("pytest", *args)


# @nox.session(python="3.8")
# def coverage(session: Session) -> None:
#     """Upload coverage data."""
#     install_with_constraints(session, "coverage[toml]", "codecov")
#     session.run("coverage", "xml", "--fail-under=0")
#     session.run("codecov", *session.posargs)


@session(name="docs-build", python="3.8")
def docs_build(session: Session) -> None:
    """Build the documentation."""
    args = session.posargs or ["-M", "html", "source", "_build"]
    if not session.posargs and "FORCE_COLOR" in environ:
        args.insert(0, "--color")

    session.install(".")
    session.install(
        "sphinx",
        "myst_parser",
    )

    build_dir = Path("_build")
    html_dir = Path("_build/html")
    output_dir = Path("docs")
    no_jekyll = output_dir / ".nojekyll"

    session.run("sphinx-build", *args)

    if output_dir.exists():
        rmtree(output_dir)

    copytree(html_dir, output_dir)
    no_jekyll.touch()

    rmtree(build_dir)
