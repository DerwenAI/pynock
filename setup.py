"""
Package set up, used for CI testing.
"""

import pathlib
import typing

import setuptools  # type: ignore


DESCRIP = """
Examples for low-level Parquet read/write in Python
""".strip()

KEYWORDS = [
    "knowledge graph",
    "parquet",
    "serialization",
]


def parse_requirements_file (filename: str) -> typing.List[ str ]:
    """parse `requirements.txt` file, stripping constraints, comments, etc."""
    reqs = []  # pylint: disable=W0621

    for line in pathlib.Path(filename).open(encoding="utf-8").readlines():
        line = line.strip()

        if line.startswith("git+"):
            pkg = line.split("#")[1].replace("egg=", "")
            line = pkg + " @ " + line
        else:
            line = line.replace(" ", "").split("#")[0]

        reqs.append(line)

    return reqs


if __name__ == "__main__":
    setuptools.setup(
        name = "nock",
        version = "1.0.0",
        license = "MIT",

        python_requires = ">=3.8",
        install_requires = parse_requirements_file("requirements.txt"),
        packages = setuptools.find_packages(exclude=[
            "dat",
            "tests",
            "venv",
        ]),

        author = "Paco Nathan",
        author_email = "paco@derwen.ai",

        description = DESCRIP,
        long_description = pathlib.Path("README.md").read_text(encoding="utf-8"),
        long_description_content_type = "text/markdown",

        keywords = ", ".join(KEYWORDS),
        classifiers = [
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "Intended Audience :: Information Technology",
            "Intended Audience :: Science/Research",
            "Topic :: Scientific/Engineering :: Artificial Intelligence",
            "Topic :: Scientific/Engineering :: Human Machine Interfaces",
            "Topic :: Scientific/Engineering :: Information Analysis",
            ],

        url = "https://github.com/DerwenAI/nock",
        zip_safe = False,
    )
