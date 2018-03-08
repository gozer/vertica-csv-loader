from setuptools import setup

setup(
    name='vertica-csv-loader',
    version='0.1.0',
    packages=['vertica_loader'],
    python_requires='>=3.4',
    install_requires=[
        'click',
        'pyodbc',
        'PyYAML'
    ],
    test_requires=[
        'nose'
    ],
    author='Aaron Wirick',
    author_email='awirick@mozilla.com',
    description='Python tool for loading CSV files into vertica via ODBC',
    entry_points='''
        [console_scripts]
        vertica-csv-loader=vertica_loader:run
    ''',
)

