from setuptools import find_packages, setup

setup(
    name="option_2_emr_in_asset_standalone_spark",
    packages=find_packages(exclude=["option_2_emr_in_asset_standalone_spark_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
