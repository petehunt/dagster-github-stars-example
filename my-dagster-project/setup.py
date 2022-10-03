from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="my_dagster_project",
        packages=find_packages(exclude=["my_dagster_project_tests"]),
        install_requires=[
            "dagster",
            "PyGithub",
            "matplotlib",
            "pandas",
            "nbconvert",
            "nbformat",
            "ipykernel",
            "jupytext",
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )
