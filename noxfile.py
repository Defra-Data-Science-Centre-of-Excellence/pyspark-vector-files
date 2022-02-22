"""Nox sessions."""
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
        # ! Ignoring numpy vulnerability as it was apparently
        # ! fixed on 2022-02-02 by https://github.com/numpy/numpy/pull/20960
        # ! but the Safety database hasn't been updated yet (EFT, 2022-02-22)
        "--ignore=44715",
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


# @nox.session(python="3.8")
# def docs(session: Session) -> None:
#     """Build the documentation."""
#     session.run("poetry", "install", "--no-dev", external=True)
#     install_with_constraints(session, "sphinx")
#     session.run("sphinx-build", "docs/source", "docs/_build")
