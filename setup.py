import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="DataframeConversionValidator",
    version="0.1.0",
    author="Douglas H. King",
    author_email="dhking@wharton.upenn.edu",
    description="Module for checking PySpark Dataframes before and after conversion.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/wharton/DataframeConversionValidator",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)