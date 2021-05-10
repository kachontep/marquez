#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "dbt-openlineage"
package_version = "0.14.1"
description = """The openlineage bigquery adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='OpenLineage Plugin Authors',
    author_email='openlineage@openlineage.com',
    url='openlinage.github.io',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/openlineage/macros/*.sql',
            'include/openlineage/dbt_project.yml',
        ]
    },
    install_requires=[
        "dbt-core==0.20.0b2",
        "sqlparse==0.4.1",
        "openlineage==0.0.1"
    ]
)
