from setuptools import find_packages, setup

setup(
    name="option_1_standalone_emr_and_spark",
    packages=find_packages(exclude=["option_1_standalone_emr_and_spark_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
